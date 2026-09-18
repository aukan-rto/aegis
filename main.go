package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Packet struct {
	To      string `json:"to"`
	From    string `json:"from"`
	Content string `json:"content"` // Contenido cifrado AES-GCM
}

type Hub struct {
	mu      sync.Mutex
	clients map[string]*websocket.Conn
}

var hub = Hub{
	clients: make(map[string]*websocket.Conn),
}

var db *sql.DB

// Inicializar la base de datos PostgreSQL (Neon)
func initDB() {
	var err error
	// Neon te proporcionará esta URL (ej: postgres://user:pass@ep-xyz.neon.tech/neondb?sslmode=require)
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		log.Fatal("[!] Error: La variable de entorno DATABASE_URL no está configurada.")
	}

	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("Error al conectar con PostgreSQL (Neon):", err)
	}

	// Verificar conexión
	if err = db.Ping(); err != nil {
		log.Fatal("Error al hacer ping a la base de datos:", err)
	}

	// En PostgreSQL usamos SERIAL en lugar de AUTOINCREMENT
	query := `
	CREATE TABLE IF NOT EXISTS offline_messages (
		id SERIAL PRIMARY KEY,
		recipient TEXT,
		sender TEXT,
		content TEXT
	);
	`
	_, err = db.Exec(query)
	if err != nil {
		log.Fatal("Error al crear la tabla offline_messages en Postgres:", err)
	}
	log.Println("[✔] Base de datos PostgreSQL (Neon) conectada e inicializada correctamente ")
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "[!] El parámetro 'username' es obligatorio", http.StatusBadRequest)
		return
	}
	// Normalizar a minusculas
	username := strings.ToLower(strings.TrimSpace(rawUsername))

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Error al conectar WebSocket:", err)
		return
	}
	defer conn.Close()

	hub.mu.Lock()
	if oldConn, exists := hub.clients[username]; exists {
		oldConn.Close()
	}
	hub.clients[username] = conn
	hub.mu.Unlock()

	log.Printf("[+] Usuario Conectado ")

	// Pausa para asegurar que el cliente terminó de abrir el socket y está listo.
	time.Sleep(100 * time.Millisecond)
	deliverOfflineMessages(username, conn)

	deliverOfflineMessages(username, conn)

	defer func() {
		hub.mu.Lock()
		if hub.clients[username] == conn {
			delete(hub.clients, username)
		}
		hub.mu.Unlock()
		log.Printf("[-] Usuario Desconectado ")
	}()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var packet Packet
		if err := json.Unmarshal(msgBytes, &packet); err != nil {
			log.Println("Error al parsear JSON:", err)
			continue
		}

		packet.From = username

		hub.mu.Lock()
		targetConn, exists := hub.clients[packet.To]
		hub.mu.Unlock()

		if exists {
			err := targetConn.WriteJSON(packet)
			if err != nil {
				log.Println("Error al enrutar paquete en vivo:", err)
			}
		} else {
			storeOfflineMessage(packet.To, packet.From, packet.Content)
			log.Printf("[OFFLINE] Destinatario no disponible. Mensaje en cola ...")
		}
	}
}

func storeOfflineMessage(recipient, sender, content string) {
	_, err := db.Exec("INSERT INTO offline_messages (recipient, sender, content) VALUES ($1, $2, $3)", recipient, sender, content)
	if err != nil {
		log.Println("Error al guardar mensaje offline en Postgres:", err)
	}
}

func deliverOfflineMessages(username string, conn *websocket.Conn) {
	rows, err := db.Query("SELECT id, sender, content FROM offline_messages WHERE recipient = $1", username)
	if err != nil {
		log.Println("Error al consultar mensajes offline:", err)
		return
	}
	defer rows.Close()

	var idsToDelete []int

	for rows.Next() {
		var id int
		var sender, content string
		if err := rows.Scan(&id, &sender, &content); err != nil {
			continue
		}

		packet := Packet{
			To:      username,
			From:    sender,
			Content: content,
		}

		if err := conn.WriteJSON(packet); err != nil {
			log.Println("Error al entregar mensaje offline:", err)
			break
		}
		idsToDelete = append(idsToDelete, id)
	}

	for _, id := range idsToDelete {
		_, _ = db.Exec("DELETE FROM offline_messages WHERE id = $1", id)
	}

	if len(idsToDelete) > 0 {
		log.Printf("[✔] Se entregaron mensajes pendientes en cola ")
	}
}

func main() {
	initDB()
	defer db.Close()

	http.HandleFunc("/ws", handleConnections)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Println("::: Aegis Server ::: corriendo en el puerto :" + port)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal("Error en el servidor: ", err)
	}
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/dignelidxdx/database"
	"github.com/dignelidxdx/events"
	"github.com/dignelidxdx/models"
	"github.com/dignelidxdx/repository"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
	"github.com/segmentio/ksuid"
)

func newRouter() (router *mux.Router) {
	router = mux.NewRouter()
	feed := FeedEndpoint{}
	router.HandleFunc("/feeds", feed.CreateFeedHandler).Methods(http.MethodPost)
	return
}

func main() {

	errEnv := godotenv.Load()
	if errEnv != nil {
		log.Fatal(errEnv)
	}

	mySQLDB := os.Getenv("DB_MYSQL_HOST")
	mySQLUser := os.Getenv("DB_MYSQL_USERNAME")
	mySQLPassword := os.Getenv("DB_MYSQL_PASSWORD")
	mySQLSchema := os.Getenv("DB_MYSQL_SCHEMA")
	natsAddress := os.Getenv("NATS_ADDRESS")

	// Conexion con MySQL
	addr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", mySQLUser, mySQLPassword, mySQLDB, mySQLSchema)
	//addr := "root:.14.Digne@tcp(localhost:3306)/alerts_test?parseTime=true"

	repo, err := database.NewMySQLRepository(addr)
	if err != nil {
		log.Fatal(err)
	}
	repository.SetRepository(repo)

	// Conexion con NATS que publica
	n, err := events.NewNats(natsAddress)
	if err != nil {
		log.Fatal(err)
	}
	events.SetEventStore(n)

	defer events.Close()

	// Enrutador y Carga de los endpoints
	router := newRouter()
	// Puerto 8081
	if err := http.ListenAndServe(":8081", router); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Server del feed Escuchando!")

}

// Handler

type createFeedRequest struct {
	Title       string `json:"title"`
	Description string `json:"description"`
}

type FeedEndpoint struct{}

func (f FeedEndpoint) CreateFeedHandler(w http.ResponseWriter, r *http.Request) {
	var req createFeedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	createdAt := time.Now().UTC()
	id, err := ksuid.NewRandom()
	if err != nil {
		http.Error(w, "failed to create user", http.StatusInternalServerError)
		return
	}
	feed := models.Feed{
		ID:          id.String(),
		Title:       req.Title,
		Description: req.Description,
		CreatedAt:   createdAt,
	}

	// Guarda en la base de datos
	if err := repository.InsertFeed(r.Context(), &feed); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// Publica mensaje a los que se subscribieron
	if err := events.PublishCreatedFeed(r.Context(), &feed); err != nil {
		log.Printf("failed to publish created feed event: %v", err)
	}

	w.WriteHeader(http.StatusCreated)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(feed)
}

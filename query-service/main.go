package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/dignelidxdx/database"
	"github.com/dignelidxdx/events"
	"github.com/dignelidxdx/models"
	"github.com/dignelidxdx/repository"
	"github.com/dignelidxdx/search"
	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

func newRouter() (router *mux.Router) {
	router = mux.NewRouter()
	router.HandleFunc("/feeds", listFeedsHandler).Methods(http.MethodGet)
	router.HandleFunc("/search", searchHandler).Methods(http.MethodGet)
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
	elasticSearchAddress := os.Getenv("ELASTICSEARCH_ADDRESS")

	// Conexion con MySQL
	addr := fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true", mySQLUser, mySQLPassword, mySQLDB, mySQLSchema)

	repo, err := database.NewMySQLRepository(addr)
	if err != nil {
		log.Fatal(err)
	}
	repository.SetRepository(repo)

	// Conexion con el Document Search o sea ElasticSearch
	es, err := search.NewElastic(fmt.Sprintf("http://%s", elasticSearchAddress))

	if err != nil {
		log.Fatal(err)
	}
	search.SetSearchRepository(es)

	defer search.Close()

	// Conexion con NATS. Se subscribe y escucha mensajes
	n, err := events.NewNats(natsAddress)
	if err != nil {
		log.Fatal(err)
	}
	err = n.OnCreatedFeed(onCreatedFeed)
	if err != nil {
		log.Fatal(err)
	}
	events.SetEventStore(n)

	defer events.Close()

	// Enrutador donde se cargan los endpoints
	router := newRouter()

	// Puerto 8083
	if err := http.ListenAndServe(":8083", router); err != nil {
		log.Fatal(err)
	}
}

// Handler

func onCreatedFeed(m events.CreatedFeedMessage) {
	feed := models.Feed{
		ID:          m.ID,
		Title:       m.Title,
		Description: m.Description,
		CreatedAt:   m.CreatedAt,
	}
	if err := search.IndexFeed(context.Background(), feed); err != nil {
		log.Println(err)
	}
}

func listFeedsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var err error
	feeds, err := repository.ListFeeds(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(feeds)
}

func searchHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	ctx := r.Context()
	query := r.URL.Query().Get("q")
	if len(query) == 0 {
		http.Error(w, "query is required", http.StatusBadRequest)
		return
	}

	feeds, err := search.SearchFeed(ctx, query)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(feeds)
}

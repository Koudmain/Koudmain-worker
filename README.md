# Koudmain Backend Worker

Un worker backend performant écrit en Go, conçu pour traiter des tâches asynchrones au sein de l'écosystème Koudmain.

## Table des matières

- [Installation](#installation)
- [Lancement](#lancement)
- [Architecture du projet](#architecture-du-projet)
- [Ajouter une nouvelle feature](#ajouter-une-nouvelle-feature)
- [Structure interne recommandée](#structure-interne-recommandée)
- [Bonnes pratiques](#bonnes-pratiques)
- [Development](#development)

## Installation

### Prérequis

- Go 1.26.2 ou supérieur
- Git

### Cloner et configurer

```bash
# Cloner le repository
git clone git@github.com:Koudmain/Koudmain-backend-worker.git
cd Koudmain-backend-worker

# Télécharger les dépendances
go mod download

# Vérifier que tout fonctionne
go build ./cmd/koudmain
```

## Lancement

### Développement local

```bash
# Lancer directement avec go run
go run ./cmd/koudmain

# Ou construire puis exécuter
go build -o bin/koudmain ./cmd/koudmain
./bin/koudmain
```

### Build pour production

```bash
# Build standard
go build -o bin/koudmain ./cmd/koudmain

# Build optimisé (plus petit binaire)
go build -ldflags="-s -w" -o bin/koudmain ./cmd/koudmain

# Voir les infos du build
go build -v ./cmd/koudmain
```



## Architecture du projet

```
Koudmain-backend-worker/
├── cmd/                        # Point d'entrée de l'application
│   └── koudmain/
│       └── main.go            # Fonction main()
│
├── internal/                   # Code privé (non réutilisable externe)
│   ├── handler/               # Logique métier (traitements)
│   │   └── task_handler.go
│   ├── service/               # Orchestration et logique applicative
│   │   └── task_service.go
│   ├── repository/            # Accès aux données (BD, API, cache)
│   │   └── task_repository.go
│   ├── config/                # Configuration de l'app
│   │   └── config.go
│   └── model/                 # Structures de données internes
│       └── task.go
│
├── pkg/                        # Code public/réutilisable
│   └── (vide pour l'instant - pour du code external-friendly)
│
├── .github/
│   └── workflows/             # CI/CD GitHub Actions
│       └── ci-lint.yaml       # Linting automatique
│
├── go.mod                      # Déclaration du module Go
├── .golangci.yaml            # Configuration linter
├── .gitignore                # Fichiers à ignorer
└── README.md                 # Ce fichier
```

### Rôle de chaque dossier

| Dossier | Rôle | Exemple |
|---------|------|---------|
| `cmd/` | Points d'entrée | Fonction `main()`, initialisation |
| `internal/handler/` | Logique métier | Traiter une tâche, valider des données |
| `internal/service/` | Orchestration | Combiner handler + repository |
| `internal/repository/` | Données | Requêtes BD, appels API externes |
| `internal/config/` | Configuration | Variables d'environnement, fichiers config |
| `internal/model/` | Structures | Structs Go (`Task`, `User`, etc.) |
| `pkg/` | Code réutilisable | Si le code doit être importé ailleurs |

## Ajouter une nouvelle feature

### Exemple : Ajouter un traitement de notification

Suivez ces étapes :

#### 1️⃣ Créer la structure de données

**`internal/model/notification.go`**
```go
package model

type Notification struct {
    ID      string
    UserID  string
    Message string
    Status  string // "pending", "sent", "failed"
}
```

#### 2️⃣ Créer le repository (accès données)

**`internal/repository/notification_repository.go`**
```go
package repository

import "koudmain-worker/internal/model"

type NotificationRepository struct {
    // Injecter votre DB, client HTTP, etc.
}

func (r *NotificationRepository) Save(n *model.Notification) error {
    // Sauvegarder en base de données
    return nil
}

func (r *NotificationRepository) GetByID(id string) (*model.Notification, error) {
    // Récupérer depuis la base
    return nil, nil
}
```

#### 3️⃣ Créer le service (orchestration)

**`internal/service/notification_service.go`**
```go
package service

import (
    "koudmain-worker/internal/model"
    "koudmain-worker/internal/repository"
)

type NotificationService struct {
    repo *repository.NotificationRepository
}

func NewNotificationService(repo *repository.NotificationRepository) *NotificationService {
    return &NotificationService{repo: repo}
}

func (s *NotificationService) SendNotification(userID, message string) error {
    notification := &model.Notification{
        UserID:  userID,
        Message: message,
        Status:  "pending",
    }
    
    return s.repo.Save(notification)
}
```

#### 4️⃣ Créer le handler (logique métier)

**`internal/handler/notification_handler.go`**
```go
package handler

import "koudmain-worker/internal/service"

type NotificationHandler struct {
    service *service.NotificationService
}

func NewNotificationHandler(service *service.NotificationService) *NotificationHandler {
    return &NotificationHandler{service: service}
}

func (h *NotificationHandler) HandleNotificationTask(userID, message string) error {
    return h.service.SendNotification(userID, message)
}
```

#### 5️⃣ Utiliser depuis main

**`cmd/koudmain/main.go`**
```go
package main

import (
    "koudmain-worker/internal/handler"
    "koudmain-worker/internal/repository"
    "koudmain-worker/internal/service"
)

func main() {
    // Initialiser les dépendances
    notifRepo := &repository.NotificationRepository{}
    notifService := service.NewNotificationService(notifRepo)
    notifHandler := handler.NewNotificationHandler(notifService)
    
    // Utiliser
    err := notifHandler.HandleNotificationTask("user123", "Hello!")
    if err != nil {
        panic(err)
    }
}
```

### Pattern : Dependency Injection

Notez l'utilisation de **constructeurs** (`NewNotificationService`) et **injection de dépendances**. C'est une bonne pratique Go :

```go
// ✅ BON : Les dépendances sont passées
func NewService(repo *Repository) *Service {
    return &Service{repo: repo}
}

// ❌ MAUVAIS : Créer les dépendances à l'intérieur
func NewService() *Service {
    repo := &Repository{} // Dur à tester !
    return &Service{repo: repo}
}
```

## Structure interne recommandée

Au fur et à mesure que votre projet grandit, enrichissez `internal/` :

```
internal/
├── handler/           # Traitement des tasks
├── service/          # Logique applicative
├── repository/       # Accès données (BD, cache, API)
├── model/           # Structures de données
├── config/          # Configuration
├── middleware/      # (Optionnel) Logging, métriques, etc.
├── util/            # (Optionnel) Fonctions utilitaires
└── errors/          # (Optionnel) Types d'erreurs personnalisés
```

### Quand utiliser `pkg/`

Utilisez `pkg/` si vous avez du code **réutilisable externement** :

```go
// Dans pkg/validation/validator.go
package validation

func ValidateEmail(email string) bool {
    // Logique partagée réutilisable
}
```

Alors d'autres projets peuvent faire :
```go
import "koudmain-worker/pkg/validation"
validation.ValidateEmail("test@example.com")
```

## Bonnes pratiques

### 1️⃣ **Exportation (Majuscules)**

```go
// ✅ Exportée (accessible de l'extérieur)
func ProcessTask() {}
type Task struct {}

// ❌ Privée (interne au package)
func processTask() {}
type task struct {}
```

### 2️⃣ **Gestion des erreurs**

```go
// ✅ BON
result, err := repository.Get(id)
if err != nil {
    return fmt.Errorf("failed to get item: %w", err)
}

// ❌ MAUVAIS
result := repository.Get(id) // Ignorer les erreurs !
```

### 3️⃣ **Nommage des interfaces**

```go
// ✅ BON : Interface minimaliste et nommée avec suffixe -er
type Repository interface {
    Save(item interface{}) error
}

// ❌ MAUVAIS : Nom trop générique
type DataStore interface {
    Save() error
}
```

### 4️⃣ **Commentaires**

```go
// ✅ BON : Explique le POURQUOI
// CloseBody ensures HTTP responses are properly closed to avoid memory leaks
func CloseBody(resp *http.Response) error {
    return resp.Body.Close()
}

// ❌ MAUVAIS : Redondant
// Close the response body
func CloseBody(resp *http.Response) error {
    return resp.Body.Close()
}
```

## Development

### Linting

```bash
# Lancer golangci-lint
golangci-lint run

# Ou via le workflow CI
git push  # Lance automatiquement le workflow GitHub Actions
```

### Build et format

```bash
# Formatter le code
go fmt ./...

# Organiser les imports
goimports -w ./...

# Vérifier les erreurs
go vet ./...

# Toutes les checks
golangci-lint run
```

### Makefile (optionnel)

Créez un `Makefile` pour simplifier :

```makefile
# Variables
BINARY_NAME="koudmain-worker"
MAIN_PATH="./cmd/koudmain/main.go"

# Commands
all:	build

build:
	go build -o $(BINARY_NAME) $(MAIN_PATH)

run:
	go run $(MAIN_PATH)

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

clean:
	go clean
	rm -rf $(BINARY_NAME)

re: clean all

.PHONY: all build run lint fmt clean re
```

Alors vous pouvez juste faire : `make build`, `make run`, `make lint`, etc. 🚀

---

## Troubleshooting

### Import not found

```
cannot find module providing package ...
```

**Solution :**
```bash
go mod tidy
go mod download
```

### Port déjà utilisé

```bash
# Trouver le process qui utilise le port
lsof -i :8080
kill -9 <PID>
```

---

Vous avez des questions sur l'architecture ou besoin de clarifications ? 🎯

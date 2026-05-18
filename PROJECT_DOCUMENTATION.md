# Documentation du projet — Koudmain Worker

Ce document décrit la structure du projet, le rôle des dossiers et fichiers principaux, la convention de tests utilisée et la politique de tests / couverture à appliquer.

## Structure générale

Racine du projet (extraits) :

- `cmd/` — points d'entrée de l'application
- `internal/` — code interne (non exporté)
  - `internal/chat/` — implémentation du hub WebSocket ([internal/chat/hub.go](internal/chat/hub.go)) et tests ([internal/chat/hub_test.go](internal/chat/hub_test.go))
  - `internal/server/` — gestion WebSocket, endpoints et tests ([internal/server/ws.go](internal/server/ws.go), [internal/server/ws_test.go](internal/server/ws_test.go))
  - `internal/repository/` — accès aux données (Redis), creation de clients et tests ([internal/repository/redis.go](internal/repository/redis.go), [internal/repository/redis_test.go](internal/repository/redis_test.go))
  - `internal/model/` — structures de données (DTOs) ([internal/model/chat.go](internal/model/chat.go))
  - `internal/handler/` — handlers métiers (ex : websocket handler)

- `pkg/` — code public / réutilisable (vide pour l'instant)
- `go.mod`, `Makefile`, `README.md` — métadonnées et commandes utiles

## Fichiers et rôles principaux

- [cmd/koudmain/main.go](cmd/koudmain/main.go) — point d'entrée, wiring des dépendances
- [internal/chat/hub.go](internal/chat/hub.go) — implementation du Hub gérant les clients WebSocket. Conçu pour être testable via l'interface `Conn`.
- [internal/chat/hub_test.go](internal/chat/hub_test.go) — tests unitaires du Hub (mock de connexion).
- [internal/server/ws.go](internal/server/ws.go) — handler HTTP -> WebSocket, authentification JWT et enregistrement dans le Hub.
- [internal/server/ws_test.go](internal/server/ws_test.go) — test d'intégration léger simulant une connexion WS et un jeton JWT.
- [internal/repository/redis.go](internal/repository/redis.go) — wrapper Redis (client, Subscribe, Ping).
- [internal/repository/redis_test.go](internal/repository/redis_test.go) — tests d'intégration utilisant `miniredis`.
- [internal/model/chat.go](internal/model/chat.go) — structure de message (DTO). Tests optionnels pour la sérialisation JSON.

Si vous ajoutez de nouveaux dossiers/fichiers, ajoutez une entrée ici pour que les futurs contributeurs comprennent le rôle.

## Convention de tests

- Emplacement : les tests unitaires doivent vivre dans le même package que le code testé et porter le suffixe `_test.go`.
- Nommage : `package foo` ou `package foo_test` selon si vous voulez tester en boîte blanche ou boîte noire.
- Types de tests :
  - Unitaires : isolés, rapides, sans dépendances externes (DB/Redis). Mockez les dépendances via interfaces.
  - Intégration : tests qui nécessitent un service externe (Redis, DB). Préférez des solutions embarquées pour CI (ex : `miniredis`, containers via `docker-compose`/testcontainers).
  - Fonctionnels / End-to-end : tests qui démarrent le serveur et valident des flux complets. À utiliser plus rarement car lents.

Commandes utiles :

```bash
# Lancer tous les tests (verbose)
go test ./... -v

# Lancer les tests d'un package
go test ./internal/chat -v

# Couverture globale
go test ./... -coverprofile=coverage.out && go tool cover -func=coverage.out | sed -n '1,200p'

# Via Makefile
make test
make test-cover
```

## Politique de tests et couverture

Objectif minimal :

- Seuil minimal par package : 60% de couverture
- Objectif recommandé (équipe) : 80%+ pour les packages critiques (`internal/chat`, `internal/repository`, `internal/server`)

Règles pratiques :

1. Les nouveaux packages doivent être fournis avec des tests couvrant au moins 60% du code.
2. Les modifications sur le code existant doivent ajouter/mettre à jour des tests pour conserver ou améliorer la couverture du package affecté.
3. Les tests d'intégration (ex : Redis) doivent pouvoir s'exécuter en CI. Utilisez `miniredis` ou des containers dédiés dans le workflow CI.
4. Les tests UI/End-to-end sont optionnels et exécutés séparément (par exemple, sous une étape `e2e` en CI).

---

## Conventions de commentaires (GoDoc)

Le projet suit les conventions GoDoc pour documenter les types et fonctions. Principes pratiques :

- Commencez par le nom : chaque commentaire de fonction/type doit commencer par le nom de l'élément documenté. Ex : "NewRedisRepository crée ...".
- Phrase complète : écrivez une phrase complète et explicative (pas seulement un mot-clé). Décrivez le but et les effets secondaires importants (fermeture, goroutines, mutex, etc.).
- Documentez les comportements non triviaux : erreurs retournées, préconditions des paramètres, invariants, effets de bord.
- Arguments / Retour : pour les fonctions publiques complexes, ajoutez une courte section "Arguments" et "Retour" expliquant les paramètres importants et les erreurs possibles.
- Exportés vs non-exportés : documentez préférentiellement les éléments exportés (API publique). Pour les éléments non exportés, commentez si la logique est complexe.
- Exemples : pour des usages importants, fournissez une fonction `ExampleXxx` dans un fichier `_test.go` afin que `godoc` et `go test` exécutent l'exemple.
- Evitez la redondance : n'écrivez pas des commentaires qui ne font que répéter la signature sans ajouter d'information.

Quand détailler les arguments et retours
- Ajoutez une mini-section `Arguments`/`Retour` si la fonction :
  - a plusieurs paramètres dont le rôle n'est pas évident ;
  - a des préconditions (ex. champ non-nil, format attendu) ;
  - retourne des erreurs spécifiques qui nécessitent une explication.

Exemple de commentaire étendu (recommandé pour fonctions publiques complexes) :

```go
// SendMessage envoie un message formaté à un destinataire spécifique.
//
// Arguments:
//   - userID: identifiant unique du destinataire.
//   - content: corps du message à envoyer.
//   - opts: options de configuration (priorité, retry).
//
// Retour:
//   - error: erreur si le destinataire n'existe pas ou si l'envoi échoue.
func SendMessage(userID int, content string, opts Options) error { ... }
```

Exemple minimal (suffisant pour fonctions simples) :

```go
// NewRedisRepository crée et configure un client Redis pointant sur host:port.
// Retourne une erreur si la connexion ne peut être établie.
func NewRedisRepository(host, port, password string) (*RedisRepository, error) { ... }
```

Outils utiles : `godoc`, `go doc`, et l'intégration avec les IDE pour afficher ces commentaires pendant le développement. Conservez la cohérence du style dans tout le projet.


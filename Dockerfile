# Stage 1: Base - Setup commun
FROM golang:1.26.2-alpine AS base

WORKDIR /usr/src/app

# Installer les dépendances communes
RUN apk add --no-cache git ca-certificates tzdata

# Copier les fichiers de dépendances
COPY go.mod go.sum ./
RUN go mod download

# Copier le code source
COPY . .

# Stage 2: Builder - Compilation
FROM base AS builder

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o bin/koudmain-worker \
    ./cmd/koudmain

# Stage 3: Dev - Développement avec hot reload
FROM base AS development

# Installer air pour le hot reload
RUN go install github.com/air-verse/air@latest

# Exposer le port
EXPOSE 3005

# Lancer air pour le hot reload
CMD ["air", "-c", ".air.toml"]

# Stage 4: Prod - Production minimaliste
FROM alpine:latest AS prod

WORKDIR /usr/src/app

# Installer les certificats SSL et timezone
RUN apk add --no-cache ca-certificates tzdata

# Créer un utilisateur non-root
RUN addgroup -g 1000 koudmain && \
    adduser -D -u 1000 -G koudmain koudmain

# Copier uniquement le binaire compilé
COPY --from=builder --chown=koudmain:koudmain /usr/src/app/bin/koudmain-worker /usr/src/app/koudmain-worker

# Utiliser l'utilisateur non-root
USER koudmain

ENTRYPOINT ["/usr/src/app/koudmain-worker"]

package model

// Platform identifie la plateforme d'un device token (valeurs alignées sur
// l'enum DeviceTokenPlatform de NestJS).
type Platform string

const (
	// PlatformIOS désigne un device token APNs.
	PlatformIOS Platform = "IOS"
	// PlatformAndroid désigne un device token FCM.
	PlatformAndroid Platform = "ANDROID"
)

// DeliveryStatus est le statut d'audit d'un envoi push.
type DeliveryStatus string

const (
	// DeliveryStatusSent indique que le provider a accepté la notification.
	DeliveryStatusSent DeliveryStatus = "Sent"
	// DeliveryStatusFailed indique que l'envoi a échoué.
	DeliveryStatusFailed DeliveryStatus = "Failed"
)

// DeliveryChannelPush est le canal enregistré pour les envois push.
const DeliveryChannelPush = "Push"

// PushPayload est le contenu de la notification affichée sur le device.
// CollapseID et Data sont optionnels.
type PushPayload struct {
	Title      string         `json:"title"`
	Body       string         `json:"body"`
	CollapseID string         `json:"collapseId,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
}

// PushJob est le contenu JSON du champ `payload` d'une entrée du stream
// Redis `push-jobs`, publiée par NestJS.
type PushJob struct {
	RecipientUserIDs []int       `json:"recipientUserIds"`
	Payload          PushPayload `json:"payload"`
	OutboxEventID    int         `json:"outboxEventId"`
	ApplicationID    *int        `json:"applicationId"`
}

// DeviceToken est un device token renvoyé par
// GET /internal/device-tokens.
type DeviceToken struct {
	UserID   int      `json:"userId"`
	Token    string   `json:"token"`
	Platform Platform `json:"platform"`
}

// NotificationDelivery est le corps envoyé à
// POST /internal/notification-deliveries pour auditer un envoi.
// ApplicationID et ErrorMessage sont sérialisés à `null` quand ils sont nil.
type NotificationDelivery struct {
	OutboxEventID int            `json:"outboxEventId"`
	ApplicationID *int           `json:"applicationId"`
	UserID        int            `json:"userId"`
	Platform      Platform       `json:"platform"`
	Channel       string         `json:"channel"`
	Status        DeliveryStatus `json:"status"`
	ErrorMessage  *string        `json:"errorMessage"`
}

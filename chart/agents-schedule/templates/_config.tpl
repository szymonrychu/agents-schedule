{{/* Mount point and file name of the orchestrator token (the Secret key is projected to "token"). */}}
{{- define "agents-schedule.tokenDir" -}}/var/run/secrets/orchestrator{{- end }}

{{/* Mount point of the admin tokens file (the Secret key is projected to "tokens"). */}}
{{- define "agents-schedule.adminTokensDir" -}}/var/run/secrets/admin{{- end }}

{{/* Name of the admin tokens Secret; fails when the admin listener is on without one. */}}
{{- define "agents-schedule.adminTokensSecret" -}}
{{- required "admin.tokensSecret.name is required when admin.enabled" .Values.admin.tokensSecret.name -}}
{{- end }}

{{/* Settings ConfigMap data; no secret values. Fails on a bad env key or a missing orchestrator url. */}}
{{- define "agents-schedule.config" -}}
SCHED_ORCHESTRATOR_URL: {{ required "orchestrator.url is required" .Values.orchestrator.url | quote }}
SCHED_ORCHESTRATOR_TOKEN_FILE: {{ printf "%s/token" (include "agents-schedule.tokenDir" .) | quote }}
SCHED_HTTP_PORT: {{ .Values.components.api.port | int | toString | quote }}
{{- if .Values.admin.enabled }}
SCHED_ADMIN_PORT: {{ .Values.admin.port | int | toString | quote }}
SCHED_ADMIN_TOKENS_FILE: {{ printf "%s/tokens" (include "agents-schedule.adminTokensDir" .) | quote }}
{{- end }}
{{- range $k, $v := .Values.env }}
{{- if not (hasPrefix "SCHED_" $k) }}
{{- fail (printf "agents-schedule: env key %q must start with SCHED_" $k) }}
{{- end }}
{{ $k }}: {{ $v | toString | quote }}
{{- end }}
{{- end }}

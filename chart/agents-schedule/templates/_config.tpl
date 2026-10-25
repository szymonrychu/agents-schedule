{{/* Mount point and file name of the orchestrator token (the Secret key is projected to "token"). */}}
{{- define "agents-schedule.tokenDir" -}}/var/run/secrets/orchestrator{{- end }}

{{/* Settings ConfigMap data; no secret values. Fails on a bad env key or a missing orchestrator url. */}}
{{- define "agents-schedule.config" -}}
SCHED_ORCHESTRATOR_URL: {{ required "orchestrator.url is required" .Values.orchestrator.url | quote }}
SCHED_ORCHESTRATOR_TOKEN_FILE: {{ printf "%s/token" (include "agents-schedule.tokenDir" .) | quote }}
SCHED_HTTP_PORT: {{ .Values.components.api.port | int | toString | quote }}
{{- range $k, $v := .Values.env }}
{{- if not (hasPrefix "SCHED_" $k) }}
{{- fail (printf "agents-schedule: env key %q must start with SCHED_" $k) }}
{{- end }}
{{ $k }}: {{ $v | toString | quote }}
{{- end }}
{{- end }}

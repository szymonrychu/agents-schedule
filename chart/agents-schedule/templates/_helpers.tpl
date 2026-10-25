{{- define "agents-schedule.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agents-schedule.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "agents-schedule.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agents-schedule.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agents-schedule.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "agents-schedule.labels" -}}
helm.sh/chart: {{ include "agents-schedule.chart" . }}
{{ include "agents-schedule.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/* Call with (dict "context" . "component" "api"). */}}
{{- define "agents-schedule.componentLabels" -}}
{{ include "agents-schedule.selectorLabels" .context }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{- define "agents-schedule.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "agents-schedule.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end }}

{{/* imagePullSecrets from values plus global, deduplicated by name. */}}
{{- define "agents-schedule.imagePullSecrets" -}}
{{- $g := ((.Values.global | default dict).imagePullSecrets) | default list }}
{{- $seen := dict }}
{{- $all := list }}
{{- range concat (.Values.imagePullSecrets | default list) $g }}
{{- if not (hasKey $seen .name) }}
{{- $_ := set $seen .name true }}
{{- $all = append $all . }}
{{- end }}
{{- end }}
{{- if $all }}
imagePullSecrets:
  {{- toYaml $all | nindent 2 }}
{{- end }}
{{- end }}

{{/* Name of the Secret holding the database URL; fails when no source is set. */}}
{{- define "agents-schedule.databaseSecretName" -}}
{{- $p := .Values.postgres -}}
{{- if and $p.url $p.existingSecret -}}
{{- fail "agents-schedule: set only one of postgres.url and postgres.existingSecret" -}}
{{- else if $p.existingSecret -}}
{{- $p.existingSecret -}}
{{- else if $p.url -}}
{{- include "agents-schedule.fullname" . }}-database
{{- else -}}
{{- fail "agents-schedule: a Postgres connection is required: set postgres.url or postgres.existingSecret" -}}
{{- end -}}
{{- end }}

{{- define "agents-schedule.databaseSecretKey" -}}
{{- if .Values.postgres.existingSecret -}}
{{- .Values.postgres.existingSecretKey | default "uri" -}}
{{- else -}}
uri
{{- end -}}
{{- end }}

{{/* Name of the Secret holding the orchestrator token; fails when none is set. */}}
{{- define "agents-schedule.tokenSecretName" -}}
{{- $o := .Values.orchestrator -}}
{{- if and $o.tokenSecret $o.tokenSecret.name -}}
{{- $o.tokenSecret.name -}}
{{- else if $o.token -}}
{{- include "agents-schedule.fullname" . }}-orchestrator
{{- else -}}
{{- fail "agents-schedule: the orchestrator token is required: set orchestrator.token or orchestrator.tokenSecret.name" -}}
{{- end -}}
{{- end }}

{{- define "agents-schedule.tokenSecretKey" -}}
{{- if and .Values.orchestrator.tokenSecret .Values.orchestrator.tokenSecret.name -}}
{{- .Values.orchestrator.tokenSecret.key | default "token" -}}
{{- else -}}
token
{{- end -}}
{{- end }}

{{/* Container env: the settings ConfigMap and the database URL from its Secret. Call with the root context. */}}
{{- define "agents-schedule.env" -}}
envFrom:
  - configMapRef:
      name: {{ include "agents-schedule.fullname" . }}-settings
env:
  - name: SCHED_DATABASE_URL
    valueFrom:
      secretKeyRef:
        name: {{ include "agents-schedule.databaseSecretName" . }}
        key: {{ include "agents-schedule.databaseSecretKey" . }}
{{- end }}

{{/* Validates Secret inputs even when no Deployment includes the env. */}}
{{- define "agents-schedule.validate" -}}
{{- $_ := include "agents-schedule.databaseSecretName" . }}
{{- $_ := include "agents-schedule.tokenSecretName" . }}
{{- end }}

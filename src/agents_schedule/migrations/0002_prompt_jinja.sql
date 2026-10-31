-- Prompts become Jinja templates. First, literal `{{`, `{%` and `{#` become `{{ '{{' }}` (etc.)
-- so they still render literally; tokens already in that escaped form, and converted
-- placeholders, are skipped. Then `{schedule_name}` and `{fired_at}` become `{{ schedule_name }}`
-- and `{{ fired_at }}`. Both steps skip converted text, so a rerun changes nothing.
UPDATE schedules
SET prompt = regexp_replace(
    regexp_replace(
        prompt,
        '(?<!\{\{ '')(\{\{|\{%|\{#)(?! ''(?:\{\{|\{%|\{#)'' \}\})(?! (?:schedule_name|fired_at) \}\})',
        '{{ ''\1'' }}',
        'g'
    ),
    '(?<!\{)\{(schedule_name|fired_at)\}(?!\})',
    '{{ \1 }}',
    'g'
);

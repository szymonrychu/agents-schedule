-- Prompts become Jinja templates: `{schedule_name}` and `{fired_at}` become `{{ schedule_name }}`
-- and `{{ fired_at }}`. Braces already doubled (a converted row) are skipped, so a rerun is a no-op.
UPDATE schedules
SET prompt = regexp_replace(prompt, '(?<!\{)\{(schedule_name|fired_at)\}(?!\})', '{{ \1 }}', 'g')
WHERE prompt ~ '(?<!\{)\{(schedule_name|fired_at)\}(?!\})';

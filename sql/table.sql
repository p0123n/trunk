CREATE TABLE public.trunk_queue (
    id bigserial PRIMARY KEY,
    name varchar(255),
    message text,
    locked_at timestamptz
);

CREATE INDEX trunk_queue_name_id ON public.trunk_queue (name, id) WHERE locked_at IS NULL;


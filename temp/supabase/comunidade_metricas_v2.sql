create or replace function public.comunidade_metricas_v2(
  p_date_from date default null,
  p_date_to   date default null
)
returns jsonb
language plpgsql
security definer
stable
as $$
declare
  v_result jsonb;
  v_excluded_email text := 'admin@tironalua.com';
begin
  if p_date_from is null then p_date_from := '2024-01-01'::date; end if;
  if p_date_to   is null then p_date_to   := current_date;       end if;

  with
  pubs as materialized (
    select
      pe.id,
      pe.tipo,
      pe.created_at,
      pe.usuario_id,
      pe.conteudo,
      pe.curtidas,
      pe.missao_id,
      pe.circle_post_id,
      pe.circle_comment_id
    from public.publicacoes_escalada pe
    where
      pe.ativo = true
      and pe.tipo in ('post', 'comentario')
      and pe.created_at >= p_date_from::timestamp
      and pe.created_at < (p_date_to + 1)::timestamp
      and lower(btrim(coalesce(pe.email, ''))) <> v_excluded_email
  ),
  totais as (
    select
      count(*) filter (where tipo = 'post')::int        as total_posts,
      count(*) filter (where tipo = 'comentario')::int   as total_comentarios,
      count(distinct missao_id) filter (where missao_id is not null)::int as total_missoes
    from pubs
  ),
  comment_counts as (
    select
      circle_post_id,
      count(*)::int as qtd
    from public.publicacoes_escalada
    where
      ativo = true
      and tipo = 'comentario'
      and circle_post_id is not null
      and lower(btrim(coalesce(email, ''))) <> v_excluded_email
    group by circle_post_id
  ),
  posts_scored as (
    select
      p.id,
      p.created_at,
      p.conteudo,
      p.curtidas,
      p.circle_post_id,
      coalesce(array_length(p.curtidas, 1), 0) as likes,
      coalesce(cc.qtd, 0) as comments,
      coalesce(array_length(p.curtidas, 1), 0) + coalesce(cc.qtd, 0) as score
    from pubs p
    left join comment_counts cc on cc.circle_post_id = p.circle_post_id
    where p.tipo = 'post'
  ),
  top_post as (
    select *
    from posts_scored
    order by score desc, created_at desc
    limit 1
  ),
  top_post_comentarios as (
    select
      c.created_at,
      c.conteudo,
      'Membro ' || dense_rank() over (order by c.usuario_id) as autor
    from pubs c
    inner join top_post tp on tp.circle_post_id = c.circle_post_id
    where c.tipo = 'comentario'
    order by c.created_at
  )
  select jsonb_build_object(
    'total_posts',        (select total_posts from totais),
    'total_comentarios',  (select total_comentarios from totais),
    'total_missoes',      (select total_missoes from totais),
    'top_post', (
      select case when tp.id is not null then
        jsonb_build_object(
          'id',         tp.id,
          'nome',       'Membro destaque',
          'created_at', tp.created_at,
          'conteudo',   tp.conteudo,
          'curtidas',   coalesce(array_length(tp.curtidas, 1), 0),
          'comentarios', tp.comments,
          'score',      tp.score,
          'lista_comentarios', coalesce((
            select jsonb_agg(jsonb_build_object(
              'autor',      tpc.autor,
              'created_at', tpc.created_at,
              'conteudo',   tpc.conteudo
            ) order by tpc.created_at)
            from top_post_comentarios tpc
          ), '[]'::jsonb)
        )
      else null end
      from (select null::uuid as id, null as nome, null::timestamptz as created_at,
            null::jsonb as conteudo, null::text[] as curtidas, 0 as comments, 0 as score) dummy
      left join top_post tp on true
    )
  )
  into v_result;

  return v_result;
end;
$$;

create or replace function public.dashboard_executivo_v2_interno(
  p_date_from date default null,
  p_date_to date default null,
  p_uf text default null,
  p_publico text default null,
  p_limit integer default 50
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_actions_min_date date;
  v_actions_max_date date;
  v_subs_min_date date;
  v_subs_max_date date;
  v_min_date date;
  v_max_date date;
  v_result jsonb;
  v_top_limit int;
  v_cep_limit int;
  v_excluded_email text := 'admin@tironalua.com';
  v_allowed_types text[] := array[
    'aula',
    'quiz',
    'download',
    'ebook',
    'artigo',
    'e-book',
    'evento',
    'aula (aparte)',
    'quiz (aparte)',
    'download (aparte)',
    'ebook (aparte)',
    'artigo (aparte)',
    'e-book (aparte)',
    'aula (hackathon)',
    'quiz (hackathon)',
    'download (hackathon)',
    'ebook (hackathon)',
    'artigo (hackathon)',
    'e-book (hackathon)'
  ];
begin
  -- range total index-friendly (evita full scan com min/max + cast)
  select a.criado_em::date
  into v_actions_min_date
  from public.acoes_usuarios a
  where a.criado_em is not null
    and exists (
      select 1
      from public."Participantes Escalada" p
      where p."Circle ID" = a.usuario_id
        and lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
    )
  order by a.criado_em asc
  limit 1;

  select a.criado_em::date
  into v_actions_max_date
  from public.acoes_usuarios a
  where a.criado_em is not null
    and exists (
      select 1
      from public."Participantes Escalada" p
      where p."Circle ID" = a.usuario_id
        and lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
    )
  order by a.criado_em desc
  limit 1;

  select p."SubscriptedAt"::date
  into v_subs_min_date
  from public."Participantes Escalada" p
  where p."SubscriptedAt" is not null
    and lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
  order by p."SubscriptedAt" asc
  limit 1;

  select p."SubscriptedAt"::date
  into v_subs_max_date
  from public."Participantes Escalada" p
  where p."SubscriptedAt" is not null
    and lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
  order by p."SubscriptedAt" desc
  limit 1;

  v_min_date := least(
    coalesce(v_actions_min_date, 'infinity'::date),
    coalesce(v_subs_min_date, 'infinity'::date)
  );
  if v_min_date = 'infinity'::date then
    v_min_date := null;
  end if;

  v_max_date := greatest(
    coalesce(v_actions_max_date, '-infinity'::date),
    coalesce(v_subs_max_date, '-infinity'::date)
  );
  if v_max_date = '-infinity'::date then
    v_max_date := null;
  end if;

  if v_min_date is null then
    return jsonb_build_object(
      'generated_at', now(),
      'kpis', jsonb_build_object(
        'inscritos', 0,
        'atendimentos', 0,
        'atendidos', 0,
        'membros_ativos', 0,
        'conversao_pct', 0,
        'professores_ativados', 0,
        'professores_recorrentes', 0,
        'recorrenciaProfessores', null,
        'recorrenciaProfessoresPct', null
      ),
      'series_daily', '[]'::jsonb,
      'by_uf', '[]'::jsonb,
      'by_publico', '[]'::jsonb,
      'by_cidade', '[]'::jsonb,
      'by_cep', '[]'::jsonb,
      'top_tipos', '[]'::jsonb,
      'top_itens', '[]'::jsonb,
      'meta', jsonb_build_object('full_range', null)
    );
  end if;

  if p_date_from is null then p_date_from := v_min_date; end if;
  if p_date_to   is null then p_date_to   := v_max_date; end if;

  v_top_limit := least(greatest(coalesce(p_limit, 50), 1), 120);
  -- payload de CEP mais enxuto para reduzir risco de 504 no gateway
  v_cep_limit := least(greatest(v_top_limit * 3, 90), 450);

  with
  geo_ceps_lookup as materialized (
    select distinct on (g.cep_norm)
      g.cep_norm,
      case
        when g.uf is null then null
        when upper(btrim(g.uf)) in ('[]', 'N/A', 'NA', 'NULL') then null
        else upper(btrim(g.uf))
      end as uf,
      case
        when g.cidade is null then null
        when upper(btrim(g.cidade)) in ('[]', 'N/A', 'NA', 'NULL', 'SEM CIDADE INFORMADA') then null
        else btrim(g.cidade)
      end as cidade,
      g.lat,
      g.lng
    from (
      select
        lpad(nullif(regexp_replace(coalesce(gc.cep, ''), '\D', '', 'g'), ''), 8, '0') as cep_norm,
        gc.uf,
        gc.cidade,
        gc.lat,
        gc.lng,
        gc.updated_at
      from public.geo_ceps gc
    ) g
    where g.cep_norm is not null
      and length(g.cep_norm) = 8
    order by g.cep_norm, g.updated_at desc nulls last
  ),
  participants_source as (
    select
      p."Circle ID" as usuario_id,
      case
        when p."UF" is null then null
        when upper(btrim(p."UF")) in ('[]', 'N/A', 'NA', 'NULL') then null
        else upper(btrim(p."UF"))
      end as uf,
      case
        when p.cidade is null then null
        when upper(btrim(p.cidade)) in ('[]', 'N/A', 'NA', 'NULL', 'SEM CIDADE INFORMADA') then null
        else btrim(p.cidade)
      end as cidade,
      p.publico as publico,
      p."SubscriptedAt"::date as sub_dt,
      nullif(regexp_replace(coalesce(p.cep, ''), '\D', '', 'g'), '') as cep_norm
    from public."Participantes Escalada" p
    where
      lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
      and
      (p_publico is null or p.publico = p_publico)
  ),
  participants_raw as materialized (
    select
      p.usuario_id,
      coalesce(p.uf, g.uf, 'N/A') as uf,
      coalesce(p.cidade, g.cidade, 'N/A') as cidade,
      p.publico,
      p.sub_dt,
      p.cep_norm
    from participants_source p
    left join geo_ceps_lookup g
      on length(coalesce(p.cep_norm, '')) = 8
      and g.cep_norm = lpad(p.cep_norm, 8, '0')
    where
      coalesce(p.uf, g.uf, 'N/A') <> 'N/A'
      and (p_uf is null or coalesce(p.uf, g.uf, 'N/A') = p_uf)
  ),
  participants_join as materialized (
    select distinct on (usuario_id)
      usuario_id,
      coalesce(uf, 'N/A') as uf,
      coalesce(cidade, 'N/A') as cidade,
      coalesce(publico, 'N/A') as publico,
      sub_dt,
      cep_norm
    from participants_raw
    where usuario_id is not null
    order by usuario_id, sub_dt desc nulls last
  ),
  participants_period as materialized (
    select *
    from participants_raw
    where sub_dt between p_date_from and p_date_to
  ),
  inscritos_periodo as (
    select count(distinct usuario_id)::int as inscritos
    from participants_period
    where usuario_id is not null
  ),
  actions_source as (
    select
      a.usuario_id,
      a.criado_em::date as dt,
      a.tipo,
      a.nome_item
    from public.acoes_usuarios a
    where
      a.criado_em >= p_date_from
      and a.criado_em < (p_date_to + 1)
      and lower(coalesce(a.tipo, '')) = any(v_allowed_types)
  ),
  actions as materialized (
    select
      a.usuario_id,
      a.dt,
      a.tipo,
      btrim(regexp_replace(lower(coalesce(a.tipo, '')), '\s*\([^)]*\)\s*$', '', 'g')) as tipo_pai,
      coalesce(a.nome_item, 'N/A') as nome_item,
      p.uf,
      p.cidade,
      p.publico,
      p.cep_norm
    from actions_source a
    join participants_join p
      on p.usuario_id = a.usuario_id
  ),
  kpi_actions as (
    select
      count(*)::int as atendimentos
    from actions
  ),
  inscritos_atendidos_periodo as (
    select count(distinct p.usuario_id)::int as atendidos
    from participants_period p
    join actions a
      on a.usuario_id = p.usuario_id
    where p.usuario_id is not null
  ),
  ativos_periodo as (
    select count(*)::int as membros_ativos
    from (
      select usuario_id
      from actions
      group by usuario_id
      having count(*) >= 2
    ) x
  ),
  professores_recorrencia as (
    select
      case
        when lower(coalesce(p_publico, '')) in ('professor', 'professores')
          then count(*)::int
        else 0
      end as professores_ativados,
      case
        when lower(coalesce(p_publico, '')) in ('professor', 'professores')
          then count(*) filter (where qtd_acoes >= 2)::int
        else 0
      end as professores_recorrentes,
      case
        when lower(coalesce(p_publico, '')) not in ('professor', 'professores') then null
        when count(*) > 0
          then round(
            (count(*) filter (where qtd_acoes >= 2)::numeric / count(*)::numeric),
            6
          )
        else null
      end as recorrencia_professores,
      case
        when lower(coalesce(p_publico, '')) not in ('professor', 'professores') then null
        when count(*) > 0
          then round(
            ((count(*) filter (where qtd_acoes >= 2)::numeric / count(*)::numeric) * 100),
            2
          )
        else null
      end as recorrencia_professores_pct
    from (
      select usuario_id, count(*)::int as qtd_acoes
      from actions
      where usuario_id is not null
      group by usuario_id
    ) x
  ),
  days as (
    select generate_series(p_date_from, p_date_to, interval '1 day')::date as dt
  ),
  series_inscr as (
    select sub_dt as dt, count(distinct usuario_id)::int as inscritos
    from participants_period
    where usuario_id is not null
    group by sub_dt
  ),
  series_atend as (
    select dt, count(*)::int as atendimentos
    from actions
    group by dt
  ),
  series_ativos as (
    select dt, count(*)::int as ativos
    from (
      select usuario_id, dt
      from actions
      group by usuario_id, dt
      having count(*) >= 2
    ) x
    group by dt
  ),
  series as (
    select
      d.dt,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      coalesce(v.ativos, 0)::int as ativos
    from days d
    left join series_inscr i on i.dt = d.dt
    left join series_atend a on a.dt = d.dt
    left join series_ativos v on v.dt = d.dt
    order by d.dt
  ),

  by_uf_inscritos as (
    select
      coalesce(uf, 'N/A') as uf,
      count(distinct usuario_id)::int as inscritos
    from participants_period
    where usuario_id is not null
    group by coalesce(uf, 'N/A')
  ),
  by_uf_atend as (
    select uf, count(*)::int as atendimentos
    from actions
    group by uf
  ),
  by_uf_ativos as (
    select uf, count(*)::int as ativos
    from (
      select uf, usuario_id
      from actions
      group by uf, usuario_id
      having count(*) >= 2
    ) x
    group by uf
  ),
  by_uf_keys as (
    select uf from by_uf_inscritos
    union
    select uf from by_uf_atend
    union
    select uf from by_uf_ativos
  ),
  by_uf as (
    select
      k.uf,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      coalesce(v.ativos, 0)::int as ativos
    from by_uf_keys k
    left join by_uf_inscritos i on i.uf = k.uf
    left join by_uf_atend a on a.uf = k.uf
    left join by_uf_ativos v on v.uf = k.uf
    order by atendimentos desc, inscritos desc
  ),

  by_publico_inscritos as (
    select
      coalesce(publico, 'N/A') as publico,
      count(distinct usuario_id)::int as inscritos
    from participants_period
    where usuario_id is not null
    group by coalesce(publico, 'N/A')
  ),
  by_publico_atend as (
    select publico, count(*)::int as atendimentos
    from actions
    group by publico
  ),
  by_publico_ativos as (
    select publico, count(*)::int as ativos
    from (
      select publico, usuario_id
      from actions
      group by publico, usuario_id
      having count(*) >= 2
    ) x
    group by publico
  ),
  by_publico as (
    select
      i.publico,
      i.inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      coalesce(v.ativos, 0)::int as ativos
    from by_publico_inscritos i
    left join by_publico_atend a on a.publico = i.publico
    left join by_publico_ativos v on v.publico = i.publico
    order by atendimentos desc, inscritos desc
  ),
  by_cidade_inscritos as (
    select
      coalesce(uf, 'N/A') as uf,
      coalesce(cidade, 'N/A') as cidade,
      count(distinct usuario_id)::int as inscritos
    from participants_period
    where usuario_id is not null
    group by coalesce(uf, 'N/A'), coalesce(cidade, 'N/A')
  ),
  by_cidade_atend as (
    select uf, cidade, count(*)::int as atendimentos
    from actions
    group by uf, cidade
  ),
  by_cidade_ativos as (
    select uf, cidade, count(*)::int as ativos
    from (
      select uf, cidade, usuario_id
      from actions
      group by uf, cidade, usuario_id
      having count(*) >= 2
    ) x
    group by uf, cidade
  ),
  cidade_acoes_tipo as (
    select
      coalesce(uf, 'N/A') as uf,
      coalesce(cidade, 'N/A') as cidade,
      tipo_pai as acao,
      count(*)::int as qtd
    from actions
    group by coalesce(uf, 'N/A'), coalesce(cidade, 'N/A'), tipo_pai
  ),
  cidade_acoes as (
    select
      uf,
      cidade,
      jsonb_agg(
        jsonb_build_object('acao', acao, 'qtd', qtd)
        order by qtd desc, acao
      ) as acoes
    from cidade_acoes_tipo
    group by uf, cidade
  ),
  by_cidade_keys as (
    select uf, cidade from by_cidade_inscritos
    union
    select uf, cidade from by_cidade_atend
    union
    select uf, cidade from by_cidade_ativos
  ),
  by_cidade as (
    select
      k.uf,
      k.cidade,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      coalesce(v.ativos, 0)::int as ativos,
      coalesce(ca.acoes, '[]'::jsonb) as acoes
    from by_cidade_keys k
    left join by_cidade_inscritos i on i.uf = k.uf and i.cidade = k.cidade
    left join by_cidade_atend a on a.uf = k.uf and a.cidade = k.cidade
    left join by_cidade_ativos v on v.uf = k.uf and v.cidade = k.cidade
    left join cidade_acoes ca on ca.uf = k.uf and ca.cidade = k.cidade
    order by atendimentos desc, inscritos desc
    limit v_top_limit
  ),
  coverage_ufs as (
    select
      count(distinct upper(btrim(uf)))::int as presentes
    from actions
    where
      nullif(btrim(coalesce(uf, '')), '') is not null
      and upper(btrim(coalesce(uf, ''))) <> 'N/A'
  ),
  coverage_cidades_uf as (
    select
      count(distinct lower(btrim(cidade)))::int as presentes
    from actions
    where
      p_uf is not null
      and uf = p_uf
      and nullif(btrim(coalesce(cidade, '')), '') is not null
      and lower(btrim(coalesce(cidade, ''))) <> 'n/a'
  ),

  -- CEP no periodo selecionado (inscricoes + atendimentos por CEP, sem dependencias anuais)
  by_cep_inscritos as (
    select
      cep_norm,
      max(nullif(uf, 'N/A')) as uf,
      max(nullif(cidade, 'N/A')) as cidade,
      count(distinct usuario_id)::int as inscritos
    from participants_period
    where
      usuario_id is not null
      and cep_norm is not null
      and length(cep_norm) = 8
    group by cep_norm
  ),
  by_cep_atend as (
    select
      cep_norm,
      max(nullif(uf, 'N/A')) as uf,
      max(nullif(cidade, 'N/A')) as cidade,
      count(*)::int as atendimentos,
      min(dt) as primeiro_atendimento_em,
      max(dt) as ultimo_atendimento_em
    from actions
    where
      cep_norm is not null
      and length(cep_norm) = 8
    group by cep_norm
  ),
  by_cep_keys as (
    select cep_norm from by_cep_inscritos
    union
    select cep_norm from by_cep_atend
  ),
  by_cep as (
    select
      k.cep_norm as cep,
      coalesce(nullif(i.uf, 'N/A'), nullif(a.uf, 'N/A'), g.uf, nullif(p_uf, ''), 'N/A') as uf,
      coalesce(nullif(i.cidade, 'N/A'), nullif(a.cidade, 'N/A'), g.cidade, 'CEP ' || k.cep_norm) as cidade,
      g.lat,
      g.lng,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      to_char(a.primeiro_atendimento_em, 'YYYY-MM-DD') as primeiro_atendimento_em,
      to_char(a.ultimo_atendimento_em, 'YYYY-MM-DD') as ultimo_atendimento_em
    from by_cep_keys k
    left join by_cep_inscritos i on i.cep_norm = k.cep_norm
    left join by_cep_atend a on a.cep_norm = k.cep_norm
    left join geo_ceps_lookup g
      on g.cep_norm = lpad(k.cep_norm, 8, '0')
    order by coalesce(a.atendimentos, 0) desc, coalesce(i.inscritos, 0) desc
    limit v_cep_limit
  ),
  top_tipos as (
    select tipo_pai as tipo, count(*)::int as qtd
    from actions
    group by tipo_pai
    order by qtd desc
    limit v_top_limit
  ),
  top_itens as (
    select nome_item, count(*)::int as qtd
    from actions
    where tipo_pai <> 'evento'
    group by nome_item
    order by qtd desc
    limit v_top_limit
  )
  select jsonb_build_object(
    'generated_at', now(),
    'kpis', jsonb_build_object(
      'inscritos', (select inscritos from inscritos_periodo),
      'atendimentos', (select atendimentos from kpi_actions),
      'atendidos', (select atendidos from inscritos_atendidos_periodo),
      'membros_ativos', (select membros_ativos from ativos_periodo),
      'professores_ativados', (select professores_ativados from professores_recorrencia),
      'professores_recorrentes', (select professores_recorrentes from professores_recorrencia),
      'recorrenciaProfessores', (select recorrencia_professores from professores_recorrencia),
      'recorrenciaProfessoresPct', (select recorrencia_professores_pct from professores_recorrencia),
      'conversao_pct',
        case
          when (select inscritos from inscritos_periodo) > 0
            then round(((select atendidos from inscritos_atendidos_periodo)::numeric / (select inscritos from inscritos_periodo)::numeric) * 100, 2)
          else 0
        end
    ),
    'series_daily', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'date', to_char(dt, 'YYYY-MM-DD'),
            'inscritos', inscritos,
            'atendimentos', atendimentos,
            'ativos', ativos
          ) order by dt
        ),
        '[]'::jsonb
      ) from series
    ),
    'by_uf', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object('uf', uf, 'inscritos', inscritos, 'atendimentos', atendimentos, 'ativos', ativos)
          order by atendimentos desc, inscritos desc
        ),
        '[]'::jsonb
      ) from by_uf
    ),
    'by_publico', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object('publico', publico, 'inscritos', inscritos, 'atendimentos', atendimentos, 'ativos', ativos)
          order by atendimentos desc, inscritos desc
        ),
        '[]'::jsonb
      ) from by_publico
    ),
    'by_cidade', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'uf', uf,
            'cidade', cidade,
            'inscritos', inscritos,
            'atendimentos', atendimentos,
            'ativos', ativos,
            'acoes', acoes
          )
          order by atendimentos desc, inscritos desc
        ),
        '[]'::jsonb
      ) from by_cidade
    ),
    'by_cep', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'cep', b.cep,
            'uf', b.uf,
            'cidade', b.cidade,
            'lat', b.lat,
            'lng', b.lng,
            'inscritos', b.inscritos,
            'atendimentos', b.atendimentos,
            'primeiro_atendimento_em', b.primeiro_atendimento_em,
            'ultimo_atendimento_em', b.ultimo_atendimento_em
          )
          order by b.atendimentos desc, b.inscritos desc
        ),
        '[]'::jsonb
      )
      from by_cep b
    ),
    'top_tipos', (
      select coalesce(
        jsonb_agg(jsonb_build_object('tipo', tipo, 'qtd', qtd) order by qtd desc),
        '[]'::jsonb
      ) from top_tipos
    ),
    'top_itens', (
      select coalesce(
        jsonb_agg(jsonb_build_object('nome_item', nome_item, 'qtd', qtd) order by qtd desc),
        '[]'::jsonb
      ) from top_itens
    ),
    'meta', jsonb_build_object(
      'full_range', jsonb_build_object(
        'from', to_char(v_min_date, 'YYYY-MM-DD'),
        'to',   to_char(v_max_date, 'YYYY-MM-DD')
      ),
      'applied_filters', jsonb_build_object(
        'date_from', to_char(p_date_from, 'YYYY-MM-DD'),
        'date_to',   to_char(p_date_to, 'YYYY-MM-DD'),
        'uf', p_uf,
        'publico', p_publico
      ),
      'coverage', jsonb_build_object(
        'ufs_com_atendimentos', coalesce((select presentes from coverage_ufs), 0),
        'ufs_com_inscritos', coalesce((select presentes from coverage_ufs), 0),
        'ufs_total', 27,
        'cidades_uf_com_atendimentos',
          case
            when p_uf is null then null
            else coalesce((select presentes from coverage_cidades_uf), 0)
          end,
        'cidades_uf_com_inscritos',
          case
            when p_uf is null then null
            else coalesce((select presentes from coverage_cidades_uf), 0)
          end
      ),
      'top_limit', v_top_limit,
      'cep_scope', 'periodo_filtrado',
      'cep_limit', v_cep_limit
    )
  )
  into v_result;

  return v_result;
end;
$$;

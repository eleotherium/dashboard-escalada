create or replace function public.dashboard_fontes_v2_interno(
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
      'by_fonte', '[]'::jsonb,
      'fontes', jsonb_build_object(
        'geral', '[]'::jsonb,
        'eventos', '[]'::jsonb,
        'multiplicadores', '[]'::jsonb,
        'embaixadores', '[]'::jsonb,
        'convites', '[]'::jsonb
      ),
      'fontes_totais', jsonb_build_object(
        'geral', jsonb_build_object('itens', 0, 'inscritos', 0, 'atendimentos', 0, 'atendidos', 0),
        'eventos', jsonb_build_object('itens', 0, 'inscritos', 0, 'atendimentos', 0, 'atendidos', 0),
        'multiplicadores', jsonb_build_object('itens', 0, 'inscritos', 0, 'atendimentos', 0, 'atendidos', 0),
        'embaixadores', jsonb_build_object('itens', 0, 'inscritos', 0, 'atendimentos', 0, 'atendidos', 0),
        'convites', jsonb_build_object('itens', 0, 'inscritos', 0, 'atendimentos', 0, 'atendidos', 0)
      ),
      'meta', jsonb_build_object('full_range', null)
    );
  end if;

  if p_date_from is null then p_date_from := v_min_date; end if;
  if p_date_to   is null then p_date_to   := v_max_date; end if;

  v_top_limit := least(greatest(coalesce(p_limit, 50), 1), 120);

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
      end as cidade
    from (
      select
        lpad(nullif(regexp_replace(coalesce(gc.cep, ''), '\D', '', 'g'), ''), 8, '0') as cep_norm,
        gc.uf,
        gc.cidade,
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
      nullif(regexp_replace(coalesce(p.cep, ''), '\D', '', 'g'), '') as cep_norm,
      to_jsonb(p) as raw_json
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
      p.cep_norm,
      p.raw_json
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
  participants_period_join as materialized (
    select distinct on (usuario_id)
      usuario_id,
      coalesce(uf, 'N/A') as uf,
      coalesce(cidade, 'N/A') as cidade,
      coalesce(publico, 'N/A') as publico,
      sub_dt,
      cep_norm,
      raw_json
    from participants_period
    where usuario_id is not null
    order by usuario_id, sub_dt desc nulls last
  ),
  actions_source as (
    select
      a.usuario_id
    from public.acoes_usuarios a
    where
      a.criado_em >= p_date_from
      and a.criado_em < (p_date_to + 1)
      and lower(coalesce(a.tipo, '')) = any(v_allowed_types)
  ),
  actions as materialized (
    select
      a.usuario_id,
      p.uf,
      p.cidade,
      p.publico
    from actions_source a
    join participants_join p
      on p.usuario_id = a.usuario_id
  ),
  participants_tag_items as (
    select distinct
      p.usuario_id,
      nullif(btrim(item.tag_text), '') as tag_text
    from participants_period_join p
    cross join lateral (
      select jsonb_array_elements_text(p.raw_json->'tags') as tag_text
      where jsonb_typeof(p.raw_json->'tags') = 'array'

      union all

      select jsonb_array_elements_text(p.raw_json->'Tags') as tag_text
      where jsonb_typeof(p.raw_json->'Tags') = 'array'

      union all

      select jsonb_array_elements_text(p.raw_json->'labels') as tag_text
      where jsonb_typeof(p.raw_json->'labels') = 'array'

      union all

      select jsonb_array_elements_text(p.raw_json->'Labels') as tag_text
      where jsonb_typeof(p.raw_json->'Labels') = 'array'

      union all

      select p.raw_json->>'tags' as tag_text
      where jsonb_typeof(p.raw_json->'tags') = 'string'

      union all

      select p.raw_json->>'Tags' as tag_text
      where jsonb_typeof(p.raw_json->'Tags') = 'string'

      union all

      select p.raw_json->>'label' as tag_text
      where jsonb_typeof(p.raw_json->'label') = 'string'

      union all

      select p.raw_json->>'Label' as tag_text
      where jsonb_typeof(p.raw_json->'Label') = 'string'
    ) item
    where
      p.usuario_id is not null
      and nullif(btrim(item.tag_text), '') is not null
  ),
  participantes_fonte_tags as (
    select
      p.usuario_id,
      nullif(
        btrim(
          regexp_replace(
            p.tag_text,
            '^fontes?\s*:\s*',
            '',
            'i'
          )
        ),
        ''
      ) as fonte
    from participants_tag_items p
    where p.tag_text ~* '^fontes?\s*:'
  ),
  participantes_fontes as (
    select distinct
      usuario_id,
      fonte
    from participantes_fonte_tags
    where fonte is not null and fonte <> ''
  ),
  by_fonte_inscritos as (
    select
      fonte,
      count(distinct usuario_id)::int as inscritos
    from participantes_fontes
    group by fonte
  ),
  by_fonte_atendimentos as (
    select
      pf.fonte,
      count(*)::int as atendimentos
    from actions a
    join participantes_fontes pf
      on pf.usuario_id = a.usuario_id
    group by pf.fonte
  ),
  by_fonte_atendidos as (
    select
      pf.fonte,
      count(distinct a.usuario_id)::int as atendidos
    from actions a
    join participantes_fontes pf
      on pf.usuario_id = a.usuario_id
    group by pf.fonte
  ),
  fonte_uf_counts as (
    select
      pf.fonte,
      coalesce(ppj.uf, 'N/A') as uf,
      count(distinct pf.usuario_id)::int as qtd
    from participantes_fontes pf
    join participants_period_join ppj
      on ppj.usuario_id = pf.usuario_id
    group by pf.fonte, coalesce(ppj.uf, 'N/A')
  ),
  fonte_publico_counts as (
    select
      pf.fonte,
      coalesce(ppj.publico, 'N/A') as publico,
      count(distinct pf.usuario_id)::int as qtd
    from participantes_fontes pf
    join participants_period_join ppj
      on ppj.usuario_id = pf.usuario_id
    group by pf.fonte, coalesce(ppj.publico, 'N/A')
  ),
  fonte_top_ufs as (
    select
      s.fonte,
      jsonb_agg(
        jsonb_build_object('uf', s.uf, 'qtd', s.qtd)
        order by s.qtd desc, s.uf
      ) as top_ufs
    from (
      select
        fuc.*,
        row_number() over (partition by fuc.fonte order by fuc.qtd desc, fuc.uf) as rn
      from fonte_uf_counts fuc
    ) s
    where s.rn <= 5
    group by s.fonte
  ),
  fonte_top_publicos as (
    select
      s.fonte,
      jsonb_agg(
        jsonb_build_object('publico', s.publico, 'qtd', s.qtd)
        order by s.qtd desc, s.publico
      ) as top_publicos
    from (
      select
        fpc.*,
        row_number() over (partition by fpc.fonte order by fpc.qtd desc, fpc.publico) as rn
      from fonte_publico_counts fpc
    ) s
    where s.rn <= 5
    group by s.fonte
  ),
  by_fonte_all as (
    select
      coalesce(i.fonte, a.fonte, ad.fonte) as fonte,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      case
        when coalesce(i.inscritos, 0) > 0
          then least(coalesce(ad.atendidos, 0), i.inscritos)::int
        else coalesce(ad.atendidos, 0)::int
      end as atendidos,
      case
        when coalesce(i.inscritos, 0) > 0
          then round((least(coalesce(ad.atendidos, 0), i.inscritos)::numeric / i.inscritos::numeric) * 100, 2)
        else 0
      end as conversao_pct,
      coalesce(tu.top_ufs, '[]'::jsonb) as top_ufs,
      coalesce(tp.top_publicos, '[]'::jsonb) as top_publicos
    from by_fonte_inscritos i
    full join by_fonte_atendimentos a
      on a.fonte = i.fonte
    full join by_fonte_atendidos ad
      on ad.fonte = coalesce(i.fonte, a.fonte)
    left join fonte_top_ufs tu
      on tu.fonte = coalesce(i.fonte, a.fonte, ad.fonte)
    left join fonte_top_publicos tp
      on tp.fonte = coalesce(i.fonte, a.fonte, ad.fonte)
    where coalesce(i.fonte, a.fonte, ad.fonte, '') <> ''
  ),
  by_fonte as (
    select
      fonte,
      inscritos,
      atendimentos,
      atendidos,
      conversao_pct,
      top_ufs,
      top_publicos
    from by_fonte_all
    order by atendimentos desc, inscritos desc, fonte
    limit v_top_limit
  ),
  participantes_familia_tags as (
    select
      p.usuario_id,
      'eventos'::text as familia,
      nullif(
        btrim(
          regexp_replace(
            p.tag_text,
            '^eventos?\s*:\s*',
            '',
            'i'
          )
        ),
        ''
      ) as label
    from participants_tag_items p
    where p.tag_text ~* '^eventos?\s*:'

    union all

    select
      p.usuario_id,
      'embaixadores'::text as familia,
      nullif(
        btrim(
          regexp_replace(
            p.tag_text,
            '^embaixad(?:or|ores)\s*:\s*',
            '',
            'i'
          )
        ),
        ''
      ) as label
    from participants_tag_items p
    where p.tag_text ~* '^embaixad(?:or|ores)\s*:'

    union all

    select
      p.usuario_id,
      'convites'::text as familia,
      nullif(
        btrim(
          regexp_replace(
            p.tag_text,
            '^convites?\s*:\s*',
            '',
            'i'
          )
        ),
        ''
      ) as label
    from participants_tag_items p
    where p.tag_text ~* '^convites?\s*:'
  ),
  participantes_fontes_especializadas as (
    select distinct
      usuario_id,
      familia,
      label
    from participantes_familia_tags
    where label is not null and label <> ''
  ),
  fontes_especializadas_inscritos as (
    select
      familia,
      label,
      count(distinct usuario_id)::int as inscritos
    from participantes_fontes_especializadas
    group by familia, label
  ),
  fontes_especializadas_atendimentos as (
    select
      pfe.familia,
      pfe.label,
      count(*)::int as atendimentos
    from actions a
    join participantes_fontes_especializadas pfe
      on pfe.usuario_id = a.usuario_id
    group by pfe.familia, pfe.label
  ),
  fontes_especializadas_atendidos as (
    select
      pfe.familia,
      pfe.label,
      count(distinct a.usuario_id)::int as atendidos
    from actions a
    join participantes_fontes_especializadas pfe
      on pfe.usuario_id = a.usuario_id
    group by pfe.familia, pfe.label
  ),
  multiplicadores_participantes as materialized (
    select distinct on (p."Circle ID")
      p."Circle ID" as usuario_id,
      lower(btrim(coalesce(p.publico, ''))) as publico_norm,
      coalesce(
        nullif(btrim(to_jsonb(p)->>'nome'), ''),
        nullif(btrim(to_jsonb(p)->>'Nome'), ''),
        nullif(btrim(to_jsonb(p)->>'name'), ''),
        nullif(btrim(to_jsonb(p)->>'Name'), ''),
        nullif(btrim(to_jsonb(p)->>'display_name'), ''),
        nullif(btrim(to_jsonb(p)->>'Display Name'), ''),
        nullif(btrim(to_jsonb(p)->>'full_name'), ''),
        nullif(btrim(to_jsonb(p)->>'Full Name'), ''),
        nullif(btrim(to_jsonb(p)->>'nome_completo'), ''),
        nullif(btrim(to_jsonb(p)->>'Nome Completo'), ''),
        p."Circle ID"::text
      ) as participante_nome
    from public."Participantes Escalada" p
    where p."Circle ID" is not null
      and lower(btrim(coalesce(p."Email", ''))) <> v_excluded_email
    order by p."Circle ID", p."SubscriptedAt" desc nulls last
  ),
  multiplicadores_base as materialized (
    select distinct on (ce.id_usuario)
      ce.id_usuario as usuario_id,
      mp.participante_nome as label,
      nullif(btrim(ce.tags[1]), '') as tracking_tag,
      lower(btrim(coalesce(ce.status, ''))) as status_norm,
      nullif(btrim(coalesce(ce.url, ce.convite_encurtado, '')), '') as link_url
    from public.convites_escalada ce
    join multiplicadores_participantes mp
      on mp.usuario_id = ce.id_usuario
    where
      ce.id_usuario is not null
      and coalesce(mp.publico_norm, '') not in ('estudante', 'estudantes')
      and lower(btrim(coalesce(ce.publico, 'interno'))) <> 'externo'
    order by
      ce.id_usuario,
      case
        when lower(btrim(coalesce(ce.status, ''))) = 'produzido' then 0
        else 1
      end,
      case
        when nullif(btrim(coalesce(ce.url, ce.convite_encurtado, '')), '') is not null then 0
        else 1
      end,
      ce.criado_em desc,
      ce.id desc
  ),
  multiplicadores_links_produzidos as (
    select
      mb.usuario_id,
      mb.label,
      mb.tracking_tag
    from multiplicadores_base mb
    where
      mb.tracking_tag is not null
      and (
        mb.status_norm = 'produzido'
        or mb.link_url is not null
      )
  ),
  multiplicadores_atribuicoes as materialized (
    select distinct
      mlp.usuario_id as multiplicador_usuario_id,
      mlp.label,
      pti.usuario_id as inscrito_usuario_id
    from multiplicadores_links_produzidos mlp
    join participants_tag_items pti
      on lower(btrim(pti.tag_text)) = lower(btrim(mlp.tracking_tag))
  ),
  multiplicadores_inscritos as (
    select
      ma.multiplicador_usuario_id as usuario_id,
      ma.label,
      count(distinct ma.inscrito_usuario_id)::int as inscritos
    from multiplicadores_atribuicoes ma
    group by ma.multiplicador_usuario_id, ma.label
  ),
  multiplicadores_atendimentos as (
    select
      ma.multiplicador_usuario_id as usuario_id,
      ma.label,
      count(*)::int as atendimentos
    from actions a
    join multiplicadores_atribuicoes ma
      on ma.inscrito_usuario_id = a.usuario_id
    group by ma.multiplicador_usuario_id, ma.label
  ),
  multiplicadores_atendidos as (
    select
      ma.multiplicador_usuario_id as usuario_id,
      ma.label,
      count(distinct a.usuario_id)::int as atendidos
    from actions a
    join multiplicadores_atribuicoes ma
      on ma.inscrito_usuario_id = a.usuario_id
    group by ma.multiplicador_usuario_id, ma.label
  ),
  multiplicadores_metricas as (
    select
      'multiplicadores'::text as familia,
      mb.label,
      coalesce(mi.inscritos, 0)::int as inscritos,
      coalesce(mat.atendimentos, 0)::int as atendimentos,
      case
        when coalesce(mi.inscritos, 0) > 0
          then least(coalesce(mad.atendidos, 0), mi.inscritos)::int
        else coalesce(mad.atendidos, 0)::int
      end as atendidos,
      case
        when coalesce(mi.inscritos, 0) > 0
          then round((least(coalesce(mad.atendidos, 0), mi.inscritos)::numeric / mi.inscritos::numeric) * 100, 2)
        else 0
      end as conversao_pct
    from multiplicadores_base mb
    left join multiplicadores_inscritos mi
      on mi.usuario_id = mb.usuario_id
     and mi.label = mb.label
    left join multiplicadores_atendimentos mat
      on mat.usuario_id = mb.usuario_id
     and mat.label = mb.label
    left join multiplicadores_atendidos mad
      on mad.usuario_id = mb.usuario_id
     and mad.label = mb.label
    where coalesce(mb.label, '') <> ''
  ),
  fontes_numericas as (
    select
      'geral'::text as familia,
      fonte as label,
      inscritos,
      atendimentos,
      atendidos,
      conversao_pct
    from by_fonte_all

    union all

    select
      mm.familia,
      mm.label,
      mm.inscritos,
      mm.atendimentos,
      mm.atendidos,
      mm.conversao_pct
    from multiplicadores_metricas mm

    union all

    select
      coalesce(i.familia, a.familia, ad.familia) as familia,
      coalesce(i.label, a.label, ad.label) as label,
      coalesce(i.inscritos, 0)::int as inscritos,
      coalesce(a.atendimentos, 0)::int as atendimentos,
      case
        when coalesce(i.inscritos, 0) > 0
          then least(coalesce(ad.atendidos, 0), i.inscritos)::int
        else coalesce(ad.atendidos, 0)::int
      end as atendidos,
      case
        when coalesce(i.inscritos, 0) > 0
          then round((least(coalesce(ad.atendidos, 0), i.inscritos)::numeric / i.inscritos::numeric) * 100, 2)
        else 0
      end as conversao_pct
    from fontes_especializadas_inscritos i
    full join fontes_especializadas_atendimentos a
      on a.familia = i.familia
     and a.label = i.label
    full join fontes_especializadas_atendidos ad
      on ad.familia = coalesce(i.familia, a.familia)
     and ad.label = coalesce(i.label, a.label)
  ),
  fontes_numericas_ranked as (
    select
      familia,
      label,
      inscritos,
      atendimentos,
      atendidos,
      conversao_pct,
      row_number() over (
        partition by familia
        order by atendimentos desc, inscritos desc, label
      ) as rn
    from fontes_numericas
    where coalesce(label, '') <> ''
  ),
  fontes_numericas_limited as (
    select
      familia,
      label,
      inscritos,
      atendimentos,
      atendidos,
      conversao_pct
    from fontes_numericas_ranked
    where rn <= v_top_limit
  ),
  fontes_totais as (
    select
      familia,
      count(*)::int as itens,
      coalesce(sum(inscritos), 0)::int as inscritos,
      coalesce(sum(atendimentos), 0)::int as atendimentos,
      coalesce(sum(atendidos), 0)::int as atendidos
    from fontes_numericas
    group by familia
  )
  select jsonb_build_object(
    'generated_at', now(),
    'by_fonte', (
      select coalesce(
        jsonb_agg(
          jsonb_build_object(
            'fonte', fonte,
            'inscritos', inscritos,
            'atendimentos', atendimentos,
            'atendidos', atendidos,
            'conversao_pct', conversao_pct,
            'top_ufs', top_ufs,
            'top_publicos', top_publicos
          )
          order by atendimentos desc, inscritos desc, fonte
        ),
        '[]'::jsonb
      ) from by_fonte
    ),
    'fontes', jsonb_build_object(
      'geral', (
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'label', label,
              'inscritos', inscritos,
              'atendimentos', atendimentos,
              'atendidos', atendidos,
              'conversao_pct', conversao_pct
            )
            order by atendimentos desc, inscritos desc, label
          ),
          '[]'::jsonb
        )
        from fontes_numericas_limited
        where familia = 'geral'
      ),
      'eventos', (
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'label', label,
              'inscritos', inscritos,
              'atendimentos', atendimentos,
              'atendidos', atendidos,
              'conversao_pct', conversao_pct
            )
            order by atendimentos desc, inscritos desc, label
          ),
          '[]'::jsonb
        )
        from fontes_numericas_limited
        where familia = 'eventos'
      ),
      'multiplicadores', (
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'label', label,
              'inscritos', inscritos,
              'atendimentos', atendimentos,
              'atendidos', atendidos,
              'conversao_pct', conversao_pct
            )
            order by atendimentos desc, inscritos desc, label
          ),
          '[]'::jsonb
        )
        from fontes_numericas_limited
        where familia = 'multiplicadores'
      ),
      'embaixadores', (
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'label', label,
              'inscritos', inscritos,
              'atendimentos', atendimentos,
              'atendidos', atendidos,
              'conversao_pct', conversao_pct
            )
            order by atendimentos desc, inscritos desc, label
          ),
          '[]'::jsonb
        )
        from fontes_numericas_limited
        where familia = 'embaixadores'
      ),
      'convites', (
        select coalesce(
          jsonb_agg(
            jsonb_build_object(
              'label', label,
              'inscritos', inscritos,
              'atendimentos', atendimentos,
              'atendidos', atendidos,
              'conversao_pct', conversao_pct
            )
            order by atendimentos desc, inscritos desc, label
          ),
          '[]'::jsonb
        )
        from fontes_numericas_limited
        where familia = 'convites'
      )
    ),
    'fontes_totais', jsonb_build_object(
      'geral', jsonb_build_object(
        'itens', coalesce((select itens from fontes_totais where familia = 'geral'), 0),
        'inscritos', coalesce((select inscritos from fontes_totais where familia = 'geral'), 0),
        'atendimentos', coalesce((select atendimentos from fontes_totais where familia = 'geral'), 0),
        'atendidos', coalesce((select atendidos from fontes_totais where familia = 'geral'), 0)
      ),
      'eventos', jsonb_build_object(
        'itens', coalesce((select itens from fontes_totais where familia = 'eventos'), 0),
        'inscritos', coalesce((select inscritos from fontes_totais where familia = 'eventos'), 0),
        'atendimentos', coalesce((select atendimentos from fontes_totais where familia = 'eventos'), 0),
        'atendidos', coalesce((select atendidos from fontes_totais where familia = 'eventos'), 0)
      ),
      'multiplicadores', jsonb_build_object(
        'itens', coalesce((select itens from fontes_totais where familia = 'multiplicadores'), 0),
        'inscritos', coalesce((select inscritos from fontes_totais where familia = 'multiplicadores'), 0),
        'atendimentos', coalesce((select atendimentos from fontes_totais where familia = 'multiplicadores'), 0),
        'atendidos', coalesce((select atendidos from fontes_totais where familia = 'multiplicadores'), 0)
      ),
      'embaixadores', jsonb_build_object(
        'itens', coalesce((select itens from fontes_totais where familia = 'embaixadores'), 0),
        'inscritos', coalesce((select inscritos from fontes_totais where familia = 'embaixadores'), 0),
        'atendimentos', coalesce((select atendimentos from fontes_totais where familia = 'embaixadores'), 0),
        'atendidos', coalesce((select atendidos from fontes_totais where familia = 'embaixadores'), 0)
      ),
      'convites', jsonb_build_object(
        'itens', coalesce((select itens from fontes_totais where familia = 'convites'), 0),
        'inscritos', coalesce((select inscritos from fontes_totais where familia = 'convites'), 0),
        'atendimentos', coalesce((select atendimentos from fontes_totais where familia = 'convites'), 0),
        'atendidos', coalesce((select atendidos from fontes_totais where familia = 'convites'), 0)
      )
    ),
    'meta', jsonb_build_object(
      'full_range', jsonb_build_object(
        'from', to_char(v_min_date, 'YYYY-MM-DD'),
        'to', to_char(v_max_date, 'YYYY-MM-DD')
      ),
      'applied_filters', jsonb_build_object(
        'date_from', to_char(p_date_from, 'YYYY-MM-DD'),
        'date_to', to_char(p_date_to, 'YYYY-MM-DD'),
        'uf', p_uf,
        'publico', p_publico
      ),
      'top_limit', v_top_limit
    )
  )
  into v_result;

  return v_result;
end;
$$;

select 
    gu.OBJECTID AS fid,
    -- r169323.nextval fid,
    ue.id id_unidade_estratigrafica,
    ue.sigla,
    he.nome_pt hierarquia,
    decode(he.id, 38, ue.nome, he.nome_pt || ' ' || ue.nome) nome,
    at.nome_pt ambiente_tectonico,
    sa.nome_pt sub_ambiente_tectonico,
    um.sigla sigla_pai,
    nvl2(um.id, decode(hm.id, 38, um.sigla || ' - ' || um.nome, um.sigla || ' - ' || hm.nome_pt || ' ' || um.nome), null) nome_pai,
    ue.legenda,
    '1:' || to_char(be.valor, 'fm999G999G999') escala,
    wm.nome_mapa mapa,
    -- monta_string('litoestratigrafia.ue_litologia', 'id_rocha', 'id_unidade_estratigrafica', null, 'bibliotecas.bb_rocha', ue.id) litotipos, -- Pegar por tabela associada
    ri.id range,
    ri.idade_min_unidade idade_min,
    ri.idade_max_unidade idade_max,
    eol.nome_pt eon_min,
    eou.nome_pt eon_max,
    erl.nome_pt era_min,
    eru.nome_pt era_max,
    sil.nome_pt sistema_min,
    siu.nome_pt sistema_max,
    epl.nome_pt epoca_min,
    epu.nome_pt epoca_max,
    -- monta_siglas_historicas(ue.id) siglas_historicas, -- Pegar por tabela associada
    sde.st_asbinary(gu.shape) geometry
    -- '1000000' grupo,
    -- gu.rowid record_hash
from litoestratigrafia.ue_geometria_unidade gu,
    litoestratigrafia.ue_unidade_estratigrafica ue,
    bibliotecas.bb_hierarquia_estratigrafica he,
    bibliotecas.bb_ambiente_tectonico at,
    bibliotecas.bb_sub_ambiente_tectonico sa,
    litoestratigrafia.ue_unidade_estratigrafica um,
    bibliotecas.bb_hierarquia_estratigrafica hm,
    bibliotecas.bb_range_idade ri,
    bibliotecas.bb_eon eol,
    bibliotecas.bb_eon eou,
    bibliotecas.bb_era erl,
    bibliotecas.bb_era eru,
    bibliotecas.bb_sistema sil,
    bibliotecas.bb_sistema siu,
    bibliotecas.bb_epoca epl,
    bibliotecas.bb_epoca epu,
    geobank.wm_mapas wm,
    bibliotecas.bb_escala be
where gu.id_mapa between 2 and 46
--and gu.rowid in (select trim(record_hash) from litoestratigrafia.t)
--and gu.rowid not in (select trim(record_hash) from litoestratigrafia.t_area_zero_geo where escala <> '1:100,000')
and gu.id_unidade_estratigrafica(+) = ue.id
and he.id = ue.id_hierarquia_estratigrafica
and at.id(+) = ue.id_ambiente_tectonico
and sa.id(+) = ue.id_sub_ambiente_tectonico
and um.id(+) = ue.id_unidade_maior
and hm.id(+) = um.id_hierarquia_estratigrafica
and ri.id = ue.id_range_idade
and eol.id(+) = ri.eon_min
and eou.id(+) = ri.eon_max
and erl.id(+) = ri.era_min
and eru.id(+) = ri.era_max
and sil.id(+) = ri.sistema_min
and siu.id(+) = ri.sistema_max
and epl.id(+) = ri.sistema_min
and epu.id(+) = ri.sistema_max
and wm.cod_mapa = gu.id_mapa
and wm.cod_escala = be.id
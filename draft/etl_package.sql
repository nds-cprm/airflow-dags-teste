create or replace 
package body etl as
  /*procedure atualiza_bd as
    type unidades_tab_type is table of litoestratigrafia.ue_unidade_estratigrafica%rowtype;
    unidades_tab unidades_tab_type := unidades_tab_type();
    type litologia_tab_type is table of litoestratigrafia.ue_litologia%rowtype;
    litologia_tab litologia_tab_type := litologia_tab_type();
    type associacao_magmatica_tab_type is table of litoestratigrafia.ue_associacao_magmatica%rowtype;
    associacao_magmatica_tab associacao_magmatica_tab_type := associacao_magmatica_tab_type();
    type datacao_tab_type is table of litoestratigrafia.ue_datacao%rowtype;
    datacao_tab datacao_tab_type := datacao_tab_type();
    type escala_tab_type is table of litoestratigrafia.ue_escala%rowtype;
    escala_tab escala_tab_type := escala_tab_type();
    type ambiente_sedimentar_tab_type is table of litoestratigrafia.ue_ambiente_sedimentar%rowtype;
    ambiente_sedimentar_tab ambiente_sedimentar_tab_type := ambiente_sedimentar_tab_type();
    type estrutura_sedimentar_tab_type is table of litoestratigrafia.ue_estrutura_sedimentar%rowtype;
    estrutura_sedimentar_tab estrutura_sedimentar_tab_type := estrutura_sedimentar_tab_type();
    type metamorfismo_tab_type is table of litoestratigrafia.ue_metamorfismo%rowtype;
    metamorfismo_tab metamorfismo_tab_type := metamorfismo_tab_type();
    type auditoria_tab_type is table of litoestratigrafia.ue_auditoria_sigla%rowtype;
    auditoria_tab auditoria_tab_type := auditoria_tab_type();
  begin
    dbms_output.put_line('iniciando a atualização do esquema litoestratigrafia...');
    -- ue_unidade_estratigrafica
    dbms_output.put_line('atualizando tabela [ue_unidade_estratigrafica]');
    execute immediate 'truncate table litoestratigrafia.ue_unidade_estratigrafica';
    select cod_unidade_estrat,bb_unidade_estrat,bb_hierarquia_estrat,data_cadastro,sigla,abrev,bb_idade_sigla,cod_amb_tec_geral,cod_sub_amb_tec,des_legenda,des_ref_bibliografica,cod_lito_hierarquia_maior,ind_formal
    bulk collect into unidades_tab
    from ue_unid_estrat@prod_lito;
    forall i in unidades_tab.first .. unidades_tab.last
    insert into litoestratigrafia.ue_unidade_estratigrafica values unidades_tab(i);
    commit;
    -- ue_litologia
    dbms_output.put_line('atualizando tabela [ue_litologia]');
    execute immediate 'truncate table litoestratigrafia.ue_litologia';
    select cod_unidade_estrat,cod_tipo_rocha,bb_classe_predom_lito,cod_litologia
    bulk collect into litologia_tab
    from ue_litologia@prod_lito;
    forall i in litologia_tab.first .. litologia_tab.last
    insert into litoestratigrafia.ue_litologia values litologia_tab(i);
    commit;
    -- ue_associacao_magmatica
    dbms_output.put_line('atualizando tabela [ue_associacao_magmatica]');
    execute immediate 'truncate table litoestratigrafia.ue_associacao_magmatica';
    select cod_unidade_estrat,cod_assoc_magm,cod_textura_ignea,bb_nivel_crustal,bb_fonte_magmatica,bb_morf_intusao,idade_sm_nd_medida,idade_sm_nd_calculada,id
    bulk collect into associacao_magmatica_tab
    from ue_assocmagmatica@prod_lito;
    forall i in associacao_magmatica_tab.first .. associacao_magmatica_tab.last
    insert into litoestratigrafia.ue_associacao_magmatica values associacao_magmatica_tab(i);
    commit;
    -- ue_datacao
    dbms_output.put_line('atualizando tabela [ue_datacao]');
    execute immediate 'truncate table litoestratigrafia.ue_datacao';
    select cod_datacao,cod_unidade_estrat,bb_crono_sistema_max,bb_crono_sistema_min,bb_eon_max,bb_eon_min,bb_epoca_max,bb_epoca_min,bb_era_max,bb_era_min,bb_metodo_geocron_max,bb_metodo_geocron_min,erro_idade_max_unidade,erro_idade_min_unidade,idade_max_unidade,idade_min_unidade,data_cadastro
    bulk collect into datacao_tab
    from ue_datacao@prod_lito;
    forall i in datacao_tab.first .. datacao_tab.last
    insert into litoestratigrafia.ue_datacao values datacao_tab(i);
    commit;
    -- ue_escala
    dbms_output.put_line('atualizando tabela [ue_escala]');
    execute immediate 'truncate table litoestratigrafia.ue_escala';
    select id_ue_escala,cod_escala,cod_unidade_estrat
    bulk collect into escala_tab
    from ue_escala@prod_lito;
    forall i in escala_tab.first .. escala_tab.last
    insert into litoestratigrafia.ue_escala values escala_tab(i);
    commit;
    -- ue_ambiente_sedimentar
    dbms_output.put_line('atualizando tabela [ue_ambiente_sedimentar]');
    execute immediate 'truncate table litoestratigrafia.ue_ambiente_sedimentar';
    select cod_unidade_estrat,cod_bb_ambsed,cod_bb_depsed,cod_bb_sistsed,id
    bulk collect into ambiente_sedimentar_tab
    from ue_estrutsed@prod_lito;
    forall i in ambiente_sedimentar_tab.first .. ambiente_sedimentar_tab.last
    insert into litoestratigrafia.ue_ambiente_sedimentar values ambiente_sedimentar_tab(i);
    commit;
    -- ue_estrutura_sedimentar
    dbms_output.put_line('atualizando tabela [ue_estrutura_sedimentar]');
    execute immediate 'truncate table litoestratigrafia.ue_estrutura_sedimentar';
    select cod_estrut_sed,id_ambiente_sedimentar
    bulk collect into estrutura_sedimentar_tab
    from ue_estrutsed_estrut@prod_lito;
    forall i in estrutura_sedimentar_tab.first .. estrutura_sedimentar_tab.last
    insert into litoestratigrafia.ue_estrutura_sedimentar values estrutura_sedimentar_tab(i);
    commit;
    -- ue_metamorfismo
    dbms_output.put_line('atualizando tabela [ue_metamorfismo]');
    execute immediate 'truncate table litoestratigrafia.ue_metamorfismo';
    select cod_unidade_estrat,cod_metamorfismo,bb_metodo_geotermobar,bb_tipo_barico,bb_nivel_crustal,bb_cod_grau_metamorf,bb_tipo_metamorf,bb_zona_falha,bb_cisalhamento,bb_trajetoria_ptt,temp_pico,erro_temp_pico,pressao_pico,erro_pressao_pico,idade_max_metam,erro_idade_max_metam,bb_metodo_geocron_max,idade_min_metam,erro_idade_min_metam,bb_metodo_geocron_min
    bulk collect into metamorfismo_tab
    from ue_metamorfismo@prod_lito;
    forall i in metamorfismo_tab.first .. metamorfismo_tab.last
    insert into litoestratigrafia.ue_metamorfismo values metamorfismo_tab(i);
    commit;
    -- ue_auditoria_sigla
    dbms_output.put_line('atualizando tabela [ue_auditoria_sigla]');
    execute immediate 'truncate table litoestratigrafia.ue_auditoria_sigla';
    select cod_unid_estrat,sigla_nova,sigla_ant,data_alteracao,login_altercao,comando,justificativa
    bulk collect into auditoria_tab
    from ue_auditoria_sigla@prod_lito;
    forall i in auditoria_tab.first .. auditoria_tab.last
    insert into litoestratigrafia.ue_auditoria_sigla values auditoria_tab(i);
    commit;
    dbms_output.put_line('atualização do esquema litoestratigrafia concluída com sucesso!');
  exception
    when others then
      dbms_output.put_line(sqlerrm);
  end atualiza_bd;*/

  function monta_siglas_historicas(p_id_unidade_estratigrafica in number) return varchar2 is
    v_sql varchar2(1000);
    v_delimiter varchar2(2);
    v_string varchar2(1000);
    v_result varchar2(4000);
    type result_record_type is ref cursor;
    c_results result_record_type;
  begin
    v_sql := 'select distinct sigla_anterior from ue_auditoria_sigla where id_unidade_estratigrafica = ' || p_id_unidade_estratigrafica || ' and comando = ''alteracao''';
    open c_results for v_sql;
    loop
		  fetch c_results into v_string;
	      exit when c_results%notfound;
        v_result :=  v_result || v_delimiter || v_string;
        v_delimiter := ', ';
    end loop;
    close c_results;
    return v_result;
  end;

  function monta_string(p_parent_table in varchar2, p_parent_join_column in varchar2, p_filter_column in varchar2, p_alternative_column in varchar2, p_child_table in varchar2, p_id in number) return varchar2 is
    v_sql varchar2(1000);
    v_delimiter varchar2(2);
    v_string varchar2(1000);
    v_result varchar2(4000);
    type result_record_type is ref cursor;
    c_results result_record_type;
  begin
    v_sql := 'select distinct t2.' || nvl(p_alternative_column, 'nome_pt') || ' from '|| p_parent_table||' t1, '|| p_child_table||' t2 where t1.'|| p_filter_column||' = '||p_id||' and t2.id = t1.'|| p_parent_join_column||' order by 1';
    open c_results for v_sql;
    loop
		  fetch c_results into v_string;
	      exit when c_results%notfound;
        v_result :=  v_result || v_delimiter || v_string;
        v_delimiter := ', ';
    end loop;
    close c_results;
    return v_result;
  end;

/*
  function to_multipolygon (g in sdo_geometry) return sdo_geometry is  
    new_ei  sdo_elem_info_array := sdo_elem_info_array();
    old_ei  sdo_elem_info_array := g.sdo_elem_info;
    old_ord sdo_ordinate_array := g.sdo_ordinates;
    i       integer;
  begin
    if g.sdo_gtype <> 2003 then
       return g;
    end if;
  
    -- MULTIPOLYGON = GTYPE 2007
    -- Copiamos o elem_info original, mas tratando-o como o primeiro polígono.
    new_ei.extend(old_ei.count);
  
    for i in 1 .. old_ei.count loop
      new_ei(i) := old_ei(i);
    end loop;
  
    -- Retorna MULTIPOLYGON válido contendo o polígono original
    return sdo_geometry(2007, g.sdo_srid, null, new_ei, old_ord);  
  end;
*/

  function to_multipolygon (g in sdo_geometry) return sdo_geometry is
    new_ei  sdo_elem_info_array := sdo_elem_info_array();
    old_ei  sdo_elem_info_array := g.sdo_elem_info;
    old_ord sdo_ordinate_array := g.sdo_ordinates;
    i       integer;
  begin
    if g.sdo_gtype <> 2003 then
       return g;  -- Já é multipoligono ou não é poligono
    end if;
  
    -- MULTIPOLYGON = GTYPE 2007
    -- Copiamos o elem_info original, mas tratando-o como o primeiro polígono.
    new_ei.extend(old_ei.count);
  
    for i in 1 .. old_ei.count loop
      new_ei(i) := old_ei(i);
    end loop;
  
    -- Retorna MULTIPOLYGON válido contendo o polígono original.
    return sdo_geometry(2007, g.sdo_srid, null, new_ei, old_ord);
  end;    

/*  
  function rectify_geometry(g in sdo_geometry) return sdo_geometry is
    v_geom mdsys.sdo_geometry;
  begin
    if sdo_geom.validate_geometry_with_context(g, 0.0002) <> 'true' then
	    begin
	      select sdo_util.rectify_geometry(g, 0.0002)	into v_geom from dual;
	      return v_geom;
      exception
	      when others then
	        return g;
	    end;
	  else
	    return g;
	  end if;
  end;
*/

  function rectify_geometry(g in sdo_geometry) return sdo_geometry is
    v_ring     sdo_geometry;
    v_linhas   sdo_geometry := null;
    v_uniao    sdo_geometry;
    v_final    sdo_geometry;
    v_numrings integer;
  begin
      -- Número total de anéis (1 = externo, >=2 = buracos)
      v_numrings := sdo_util.getnumelem(g);
  
      v_linhas := null;
  
      -- Extrair cada anel como geometry e transformar em linestring.
      for i in 1 .. v_numrings loop
        v_ring := sdo_util.extract(g, i);
  
        if v_linhas is null then
          v_linhas := sdo_geom.sdo_union(v_ring, v_ring, 0.000002);
        else
          v_linhas := sdo_geom.sdo_union(v_linhas, v_ring, 0.000002);
        end if;
      end loop;
  
      -- Reconstruir o polígono a partir das linhas.
      v_uniao := sdo_geom.sdo_union(v_linhas, v_linhas, 0.000002);
  
      -- Buffer(0) para formar o polígono válido.
      v_final := sdo_geom.sdo_buffer(v_uniao, 0, 0.000002);
      
      -- Caso a retificação resulte numa geometria que não é polígono ou multi-polígono, retorna ela mesma sem realizar qualquer retificação.
      if (v_final.sdo_gtype not in (2003, 2007)) then
        v_final := g;
      end if;
      
      return v_final;
  end;

  procedure carrega_layer(p_layer in varchar2) is
    cursor c_unidades_all(p_escala number) is
      select r169323.nextval fid,
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
             monta_string('litoestratigrafia.ue_litologia', 'id_rocha', 'id_unidade_estratigrafica', null, 'bibliotecas.bb_rocha', ue.id) litotipos,
             ri.id range,
             ri.idade_max_unidade idade_max,
             eol.nome_pt eon_min,
             eou.nome_pt eon_max,
             erl.nome_pt era_min,
             eru.nome_pt era_max,
             sil.nome_pt sistema_min,
             siu.nome_pt sistema_max,
             epl.nome_pt epoca_min,
             epu.nome_pt epoca_max,
             monta_siglas_historicas(ue.id) siglas_historicas,
             gu.shape,
             decode(p_escala, 4, '250000', 5, '100000', 17, '150000', 6, '50000', 7, '25000') grupo,
             ri.idade_min_unidade idade_min,
             gu.rowid record_hash
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
      where gu.id_mapa in (select cod_mapa from geobank.wm_mapas where cod_escala = p_escala and cod_grupo <> 3 and cod_mapa not in (188, 189, 270))
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
	    and wm.cod_grupo <> 3;

    cursor c_unidades_estados_all is
      select r169323.nextval fid,
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
             monta_string('litoestratigrafia.ue_litologia', 'id_rocha', 'id_unidade_estratigrafica', null, 'bibliotecas.bb_rocha', ue.id) litotipos,
             ri.id range,
             ri.idade_max_unidade idade_max,
             eol.nome_pt eon_min,
             eou.nome_pt eon_max,
             erl.nome_pt era_min,
             eru.nome_pt era_max,
             sil.nome_pt sistema_min,
             siu.nome_pt sistema_max,
             epl.nome_pt epoca_min,
             epu.nome_pt epoca_max,
             monta_siglas_historicas(ue.id) siglas_historicas,
             gu.shape,
             'ESTADOS' grupo,
             ri.idade_min_unidade idade_min,
             gu.rowid record_hash
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
      where gu.id_mapa in (select cod_mapa from geobank.wm_mapas where cod_grupo = 3)
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
      and wm.cod_escala = be.id;

    cursor c_unidades_1000000_all is
      select r169323.nextval fid,
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
             monta_string('litoestratigrafia.ue_litologia', 'id_rocha', 'id_unidade_estratigrafica', null, 'bibliotecas.bb_rocha', ue.id) litotipos,
             ri.id range,
             ri.idade_max_unidade idade_max,
             eol.nome_pt eon_min,
             eou.nome_pt eon_max,
             erl.nome_pt era_min,
             eru.nome_pt era_max,
             sil.nome_pt sistema_min,
             siu.nome_pt sistema_max,
             epl.nome_pt epoca_min,
             epu.nome_pt epoca_max,
             monta_siglas_historicas(ue.id) siglas_historicas,
             gu.shape,
             '1000000' grupo,
             ri.idade_min_unidade idade_min,
             gu.rowid record_hash
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
      and wm.cod_escala = be.id;
      
    cursor c_unidades_2500000_all is
      select r169323.nextval fid,
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
             monta_string('litoestratigrafia.ue_litologia', 'id_rocha', 'id_unidade_estratigrafica', null, 'bibliotecas.bb_rocha', ue.id) litotipos,
             ri.id range,
             ri.idade_max_unidade idade_max,
             eol.nome_pt eon_min,
             eou.nome_pt eon_max,
             erl.nome_pt era_min,
             eru.nome_pt era_max,
             sil.nome_pt sistema_min,
             siu.nome_pt sistema_max,
             epl.nome_pt epoca_min,
             epu.nome_pt epoca_max,
             monta_siglas_historicas(ue.id) siglas_historicas,
             gu.shape,
             '2500000' grupo,
             ri.idade_min_unidade idade_min,
             gu.rowid record_hash
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
      where gu.id_mapa = 3478
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
      and wm.cod_escala = be.id;
      
    v_nextval number;
    v_record_count number := 0;
  begin
    -- Resetar a sequence.
    execute immediate 'select r169323.nextval from dual' into v_nextval;
    execute immediate 'alter sequence r169323 increment by -' || v_nextval || ' minvalue 0';
    execute immediate 'select r169323.nextval from dual' into v_nextval;
    execute immediate 'alter sequence r169323 increment by 1 minvalue 0 nomaxvalue';
    execute immediate 'truncate table litoestratigrafia.ue_layer';
    for r_unidades in c_unidades_estados_all loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_all(7) loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_all(6) loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_all(5) loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_all(17) loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_all(4) loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    v_record_count := 0;
    for r_unidades in c_unidades_1000000_all loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
    for r_unidades in c_unidades_2500000_all loop
       insert /*+ APPEND PARALLEL(4) */ into litoestratigrafia.ue_layer values r_unidades;
       v_record_count := v_record_count + 1;
       if mod(v_record_count, 1000) = 0 then
         commit;
       end if;
    end loop;
    commit;
  end;

  procedure carrega_layers is
  begin
    carrega_layer('UE_LAYER');
    --carrega_layer_geoserver;
  end;
  
  procedure carrega_layer_geoserver is
    -- Parâmetros de controle.
    l_fetch_limit constant pls_integer := 100;  -- Leitura de registros por lote.
    l_commit_batch constant pls_integer := 100;  -- Commits de inserts por lote.
  
    type t_unidades is table of litoestratigrafia.ue_layer%rowtype;
    v_unidades t_unidades;
  
    type t_sdo is table of mdsys.sdo_geometry;
    v_sdos t_sdo;
    
    type t_ids is table of number;
    v_ids t_ids;
    
    l_batch_start number;
    l_batch_end number;
  begin
    update /*+ APPEND PARALLEL(4) */ geoserver.ue_layer g
    set fid = (select objectid from litoestratigrafia.ue_layer l where g.record_hash = l.record_hash);
    commit;
    loop
      -- Lê em blocos de até <l_fetch_limit> registros.
      select *
      bulk collect into v_unidades
      from litoestratigrafia.ue_layer l
      where not exists (select 1 from geoserver.ue_layer g where trim(g.record_hash) = trim(l.record_hash))
      --and record_hash in (select record_hash from t)
      and rownum <= l_fetch_limit;
  
      exit when v_unidades.count = 0;
  
      v_sdos := t_sdo();
      v_sdos.extend(v_unidades.count);
      
      v_ids := t_ids();
      v_ids.extend(v_unidades.count);
  
      -- Conversão SDE para SDO.
      for i in 1 .. v_unidades.count loop
        begin
          --select etl.to_multipolygon(etl.rectify_geometry(sdo_util.from_wkbgeometry(sde.st_asbinary(v_unidades(i).shape)))) into v_sdos(i) from dual;
          select sdo_util.rectify_geometry(sdo_util.from_wkbgeometry(sde.st_asbinary(v_unidades(i).shape)), 0.000000008983153) into v_sdos(i) from dual;
          v_sdos(i).sdo_srid := 4326;
        exception
          when others then
            v_sdos(i) := null; -- Ignora as geometrias problemáticas.
        end;
        v_ids(i) := v_unidades(i).objectid;
      end loop;
  
      -- Inserção e commit em lotes de <l_commit_batch>.
      l_batch_start := 1;
      while l_batch_start <= v_unidades.count loop
        l_batch_end := least(l_batch_start + l_commit_batch - 1, v_unidades.count);
        forall i in l_batch_start .. l_batch_end save exceptions
          insert /*+ APPEND PARALLEL(4) */ into geoserver.ue_layer
          values (
            v_unidades(i).objectid,
            v_unidades(i).id_unidade_estratigrafica,
            v_unidades(i).sigla,
            v_unidades(i).hierarquia,
            v_unidades(i).nome,
            v_unidades(i).ambiente_tectonico,
            v_unidades(i).sub_ambiente_tectonico,
            v_unidades(i).sigla_pai,
            v_unidades(i).nome_pai,
            v_unidades(i).legenda,
            v_unidades(i).escala,
            v_unidades(i).mapa,
            v_unidades(i).litotipos,
            v_unidades(i).range,
            v_unidades(i).idade_min,
            v_unidades(i).idade_max,
            v_unidades(i).eon_min,
            v_unidades(i).eon_max,
            v_unidades(i).era_min,
            v_unidades(i).era_max,
            v_unidades(i).sistema_min,
            v_unidades(i).sistema_max,
            v_unidades(i).epoca_min,
            v_unidades(i).epoca_max,
            v_unidades(i).siglas_historicas,
            v_unidades(i).grupo,
            v_sdos(i),
            trim(v_unidades(i).record_hash)
          );
        commit;
        l_batch_start := l_batch_end + 1;
      end loop;
      
      -- Limpa a memória.
      v_unidades.delete;
      v_sdos.delete;
      v_ids.delete;
    end loop;
    -- Deleta os registros que deram erro na conversão da geometria.
    delete from geoserver.ue_layer where geometry is null;
    commit;
  exception
    when others then
      for j in 1 .. sql%bulk_exceptions.count loop
        dbms_output.put_line('Erro no registro UE_LAYER(OBJECTID=' || v_ids(sql%bulk_exceptions(j).error_index) || '): ' || sqlerrm(-sql%bulk_exceptions(j).error_code));
      end loop;
      rollback;
  end;
  
  procedure converte_layer_to_multipolygon is
    -- Parâmetros de controle.
    l_fetch_limit constant pls_integer := 100;  -- Leitura de registros por lote.
    l_commit_batch constant pls_integer := 100;  -- Commits de inserts por lote.
      
    type unidades_2003 is record(id_unidade_estratigrafica geoserver.ue_layer.id_unidade_estratigrafica%type, geometry geoserver.ue_layer.geometry%type);    
  
    type t_unidades_2003 is table of unidades_2003;
    v_unidades_2003 t_unidades_2003;
  
    type t_sdo_2007 is table of mdsys.sdo_geometry;
    v_sdos_2007 t_sdo_2007;
    
    l_batch_start number;
    l_batch_end number;
  begin
    loop
      -- Lê em blocos de até <l_fetch_limit> registros.
      select id_unidade_estratigrafica, geometry
      bulk collect into v_unidades_2003
      from geoserver.ue_layer t
      where t.geometry.sdo_gtype = 2003
      and rownum <= l_fetch_limit;
  
      exit when v_unidades_2003.count = 0;
  
      v_sdos_2007 := t_sdo_2007();
      v_sdos_2007.extend(v_unidades_2003.count);
  
      -- Conversão SDE para SDO.
      for i in 1 .. v_unidades_2003.count loop
        begin
          select to_multipolygon(v_unidades_2003(i).geometry) into v_sdos_2007(i) from dual;
        exception
          when others then
            v_sdos_2007(i) := null; -- Ignora as geometrias problemáticas.
        end;
      end loop;
  
      -- Update e commit em lotes de <l_commit_batch>.
      l_batch_start := 1;
      while l_batch_start <= v_unidades_2003.count loop
        l_batch_end := least(l_batch_start + l_commit_batch - 1, v_unidades_2003.count);
        forall i in l_batch_start .. l_batch_end save exceptions
          update /*+ APPEND PARALLEL(4) */ geoserver.ue_layer
          set geometry = v_sdos_2007(i)
          where id_unidade_estratigrafica = v_unidades_2003(i).id_unidade_estratigrafica;
        commit;
        l_batch_start := l_batch_end + 1;
      end loop;

      -- Limpa a memória.
      v_unidades_2003.delete;
      v_sdos_2007.delete;
    end loop;
  exception
    when others then
      rollback;
  end;  
  
  procedure update_layer_geometries is
    cursor c is
      select record_hash, sdo_util.rectify_geometry(sdo_util.from_wkbgeometry(sde.st_asbinary(shape)), 0.000000008983153) as shape
      from litoestratigrafia.ue_layer u
      where exists (select 1 from geoserver.ue_layer g where g.record_hash = u.record_hash);
      
    /*cursor c is
      select trim(record_hash) record_hash, sdo_util.rectify_geometry(sdo_util.from_wkbgeometry(sde.st_asbinary(shape)), 0.000000008983153) as shape
      from t where trim(record_hash) is not null;*/
  
    type t_hash_tab is table of litoestratigrafia.ue_layer.record_hash%type;
    type t_geom_tab is table of sdo_geometry;
    type t_flag_tab is table of pls_integer index by pls_integer;
  
    v_hashes t_hash_tab;
    v_geoms  t_geom_tab;
    v_failed  t_flag_tab; -- marca falhas no FORALL
  
    v_qtd number := 0;
    v_batch constant pls_integer := 100;
  begin
    open c;
    loop
      fetch c bulk collect into v_hashes, v_geoms limit v_batch;
      exit when v_hashes.count = 0;
      
      v_failed.delete;
  
      -- Ajuste de SRID em lote.
      for i in 1 .. v_geoms.count loop
          v_geoms(i).sdo_srid := 4326;
      end loop;
  
      begin
        forall i in 1 .. v_hashes.count save exceptions
          update geoserver.ue_layer g
          set geometry = v_geoms(i)
          where g.record_hash = v_hashes(i)
          and v_geoms(i) is not null;
      exception
        when others then
          for j in 1 .. sql%bulk_exceptions.count loop
            v_failed(sql%bulk_exceptions(j).error_index) := 1;
            dbms_output.put_line( 'Erro em record_hash: ' || v_hashes(sql%bulk_exceptions(j).error_index) || ' ORA-' || sql%bulk_exceptions(j).error_code);
          end loop;
      end;
      
      v_qtd := v_qtd + v_hashes.count;
  
      if mod(v_qtd, 1000) = 0 then
        dbms_output.put_line(v_qtd || ' registros processados...');
      end if;
      commit;
    end loop;
    close c;
  end;  
end etl;
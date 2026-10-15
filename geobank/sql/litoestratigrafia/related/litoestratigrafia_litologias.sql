  SELECT t1.id_unidade_estratigrafica, t2.nome_pt 
    FROM litoestratigrafia.ue_litologia t1, 
         bibliotecas.bb_rocha t2 
   WHERE t2.id = t1.id_rocha 
ORDER BY 1, 2 ASC

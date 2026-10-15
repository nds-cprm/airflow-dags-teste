SELECT DISTINCT id_unidade_estratigrafica, sigla_anterior /*, COMANDO*/ FROM LITOESTRATIGRAFIA.UE_AUDITORIA_SIGLA 
          WHERE /*id_unidade_estratigrafica = 100 AND*/ lower(comando) = 'alteracao'
ORDER BY 1, 2
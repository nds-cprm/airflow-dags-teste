SELECT 
    ue.id id_unidade_estratigrafica,
    ue.R, 
    ue.G, 
    ue.B
FROM litoestratigrafia.ue_unidade_estratigrafica ue
WHERE ue.R IS NOT NULL 
   OR ue.G IS NOT NULL 
   OR ue.B IS NOT NULL 
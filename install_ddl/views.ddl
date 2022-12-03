

CREATE OR REPLACE VIEW zv_etl as
SELECT etl_name
    ,replace(coalesce(stepnum,LEFT(sqlfile,POSITION('.' in sqlfile)-1)),'_','') as stepnum
    ,steptablename
    ,table_or_view
    ,estrowcount
    ,sqlfile
    ,sql_to_run
    ,note,
    dtm
FROM z_etl;



CREATE OR REPLACE VIEW vetl_branch_raw (root, branch, twig, bud, vein, seed) AS  SELECT root_etls.root,
    branches.branch,
    twigs.twig,
    buds.bud,
    veins.vein,
    seeds.seed
   FROM (((((( SELECT z_etl_dependencies.etl_name AS root
           FROM z_etl_dependencies
          GROUP BY z_etl_dependencies.etl_name
         HAVING (sum(
                CASE
                    WHEN ((z_etl_dependencies.etl_required) = 'raw') THEN 1
                    ELSE 0
                END) = count(*))) root_etls
     LEFT JOIN ( SELECT DISTINCT z_etl_dependencies.etl_name AS branch,
            z_etl_dependencies.etl_required
           FROM z_etl_dependencies) branches ON (((root_etls.root) = (branches.etl_required))))
     LEFT JOIN ( SELECT DISTINCT z_etl_dependencies.etl_name AS twig,
            z_etl_dependencies.etl_required
           FROM z_etl_dependencies) twigs ON (((branches.branch) = (twigs.etl_required))))
     LEFT JOIN ( SELECT DISTINCT z_etl_dependencies.etl_name AS bud,
            z_etl_dependencies.etl_required
           FROM z_etl_dependencies) buds ON (((twigs.twig) = (buds.etl_required))))
     LEFT JOIN ( SELECT DISTINCT z_etl_dependencies.etl_name AS vein,
            z_etl_dependencies.etl_required
           FROM z_etl_dependencies) veins ON (((buds.bud) = (veins.etl_required))))
     LEFT JOIN ( SELECT DISTINCT z_etl_dependencies.etl_name AS seed,
            z_etl_dependencies.etl_required
           FROM z_etl_dependencies) seeds ON (((veins.vein) = (seeds.etl_required))));

CREATE OR REPLACE VIEW vetl_branches (root, branch, twig, bud, vein, seed) AS  SELECT a.root,
    a.branch,
    a.twig,
    a.bud,
    a.vein,
    a.seed
   FROM ((((((((((vetl_branch_raw a
     LEFT JOIN vetl_branch_raw b ON (((a.branch) = (b.twig))))
     LEFT JOIN vetl_branch_raw c ON (((a.branch) = (c.bud))))
     LEFT JOIN vetl_branch_raw d ON (((a.branch) = (d.vein))))
     LEFT JOIN vetl_branch_raw e ON (((a.branch) = (e.seed))))
     LEFT JOIN vetl_branch_raw f ON (((a.twig) = (f.bud))))
     LEFT JOIN vetl_branch_raw g ON (((a.twig) = (g.vein))))
     LEFT JOIN vetl_branch_raw h ON (((a.twig) = (h.seed))))
     LEFT JOIN vetl_branch_raw i ON (((a.bud) = (i.vein))))
     LEFT JOIN vetl_branch_raw j ON (((a.bud) = (j.seed))))
     LEFT JOIN vetl_branch_raw k ON (((a.vein) = (k.seed))))
  WHERE ((b.twig IS NULL) AND (c.bud IS NULL) AND (d.vein IS NULL) AND (e.seed IS NULL) AND (f.bud IS NULL) AND (g.vein IS NULL) AND (h.seed IS NULL) AND (i.vein IS NULL) AND (j.seed IS NULL) AND (k.seed IS NULL))
  ORDER BY a.root;


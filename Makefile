DATABRICKS_DB = jigsaw_vrdc_aws.export_35ff1fde_0a9c_4d12_ad09_f9fb513ff566
SPARK_DB = export_35ff1fde_0a9c_4d12_ad09_f9fb513ff566
SPARK_HOST = sc://titan.jsaw.io:15002
DATABRICKS_PROFILE = DEFAULT
TABLES = attrition baseline_characteristics cohort cohort_data_dictionary events events_data_dictionary events_list metadata outcomes vocabulary_summary
SPARK_FILES = $(patsubst %,spark/%.parquet,$(TABLES))
DATABRICKS_FILES = $(patsubst %,databricks/%.parquet,$(TABLES))
SPARK_CSV = $(patsubst %.parquet,%.csv,$(SPARK_FILES))
DATABRICKS_CSV = $(patsubst %.parquet,%.csv,$(DATABRICKS_FILES))
SPARK_SORTED_CSV = $(patsubst %.csv,%.sort.csv,$(SPARK_CSV))
DATABRICKS_SORTED_CSV = $(patsubst %.csv,%.sort.csv,$(DATABRICKS_CSV))

databricks/%.parquet::
	mkdir -p databricks ; \
	sparkopy --databricks-profile $(DATABRICKS_PROFILE) --database $(DATABRICKS_DB) --table $* --output $@

spark/%.parquet::
	mkdir -p spark ; \
	sparkopy --spark-uri $(SPARK_HOST) --database $(SPARK_DB) --table $* --output $@

%.csv : %.parquet
	nix-shell -p parquet-tools --command "parquet-tools csv $<" > $@

%.sort.csv : %.csv
	sort $< > $@

diffs : $(SPARK_SORTED_CSV) $(DATABRICKS_SORTED_CSV)
	for p in $(TABLES); do \
		diff spark/$$p.sort.csv databricks/$$p.sort.csv; \
	done

.PHONY : all
all : $(SPARK_CSV) $(DATABRICKS_CSV)

.PHONY : clean
clean :
	rm -rf spark databricks
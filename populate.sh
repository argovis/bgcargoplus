for file in /tmp/bap/*Sprof_processed.nc; do
    python parse.py "$file"
done
python summaries.py

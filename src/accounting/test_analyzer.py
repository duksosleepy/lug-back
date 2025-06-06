import os
from pathlib import Path

from tantivy import (
    Document,
    Filter,
    Index,
    Query,
    SchemaBuilder,
    TextAnalyzerBuilder,
    Tokenizer,
)

# Create custom analyzer with ASCII folding
vietnamese_analyzer = (
    TextAnalyzerBuilder(Tokenizer.simple())
    .filter(Filter.ascii_fold())
    .filter(Filter.lowercase())
    .build()
)
index_path = Path("index/test_analyzer")
os.makedirs(index_path, exist_ok=True)
schema_builder = SchemaBuilder()

# Use in schema
schema_builder.add_text_field(
    "name", stored=True, tokenizer_name="vietnamese_normalized"
)
schema = schema_builder.build()
index = Index(schema, path=str(index_path))
# Register the analyzer
index.register_tokenizer("vietnamese_normalized", vietnamese_analyzer)


# tokens = vietnamese_analyzer.analyze("Công Ty Lớn")
# print(tokens)

writer = index.writer()
test_companies = [
    {
        "name": "Tiền ngoại tệ - BIDV CN Chợ Lớn - 14110370004614",
    },
    {
        "name": "Công Ty TNHH Thương Mại Lớn",
    },
    {
        "name": "Công Ty Cổ Phần Công Nghệ Lon",
    },
]

for company in test_companies:
    writer.add_document(Document.from_dict(company))
writer.commit()
writer.wait_merging_threads()
index.reload()

search_term = "Tiền ngoại tệ - BIDV CN Chợ Lớn - 14110370004614"
processed_terms = vietnamese_analyzer.analyze(search_term)  # Returns ["lon"]
processed_term = processed_terms[0] if processed_terms else search_term
print(processed_terms)

query1 = index.parse_query("Mai lon", ["name"])
query2 = Query.regex_query(index.schema, "name", f".*{processed_term}.*")

searcher = index.searcher()
results1 = searcher.search(query1, 10)
doc = searcher.doc(results1.hits[0][1])
print(doc.get_first("name"))

results2 = searcher.search(query2, 10)
doc = searcher.doc(results2.hits[0][1])
print(doc.get_first("name"))

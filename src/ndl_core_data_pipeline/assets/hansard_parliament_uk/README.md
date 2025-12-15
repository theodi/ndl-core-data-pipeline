This source is not crawled using a dagster pipeline. Instead, data is downloaded directly using `rsync` command

Source: 
- https://parser.theyworkforyou.com
- https://parser.theyworkforyou.com/hansard.html
- Debates: https://www.theyworkforyou.com/pwdata/scrapedxml/debates/

- Parser (Transforms HoC speeches from XML to json list file.): https://github.com/pournaki/houseofcommons-parser/blob/master/src/parser.py

To download all Commons main chamber debates from January 2025, you would use something like:

In the project root folder, run:

Debates:
```bash
mkdir -p ./data/raw/hansard_gov_uk
rsync -az --progress --exclude '.svn' --exclude 'tmp/' --relative 'data.theyworkforyou.com::parldata/scrapedxml/debates/debates2025-*' ./data/raw/hansard_gov_uk/
```

Written Questions and Answers:
```bash
mkdir -p ./data/raw/hansard_gov_uk
rsync -az --progress --exclude '.svn' --exclude 'tmp/' --relative 'data.theyworkforyou.com::parldata/scrapedxml/wrans/answers2025-*' ./data/raw/hansard_gov_uk/
```

Lords Written Questions and Answers:
```bash
mkdir -p ./data/raw/hansard_gov_uk
rsync -az --progress --exclude '.svn' --exclude 'tmp/' --relative 'data.theyworkforyou.com::parldata/scrapedxml/lordswrans/lordswrans2025-*' ./data/raw/hansard_gov_uk/
```

To convert the XML files to JSON format, you can use the parser script:

```
src/ndl_core_data_pipeline/assets/hansard_parliament_uk/parser.py
```

# Data Source Details

https://www.theyworkforyou.com/pwdata/scrapedxml/

## What the folders mean

### **`wrans/` — Written Answers (House of Commons)**

* MPs submit written questions to government departments
* Ministers provide written answers
* Highly factual, structured, policy-focused

### **`lordswrans/` — Written Answers (House of Lords)**

* Same as `wrans`, but for the House of Lords
* Often more technical or long-term policy focused
* Less constituency-specific, more thematic

### **`wms/` — Written Ministerial Statements (Commons)**

* Formal written statements by ministers
* Used to announce:

  * policy changes
  * updates
  * reports
  * government positions

**Important:**
These often contain **authoritative summaries of policy**.

### **`lordswms/` — Written Ministerial Statements (Lords)**

* Same as `wms`, but presented to the House of Lords
* Sometimes mirrors Commons WMS; sometimes Lords-specific


### **`westminhall/` — Westminster Hall debates**

* Secondary debating chamber of the House of Commons
* Topics often:

  * constituency issues
  * specific policies
  * public petitions
* Less adversarial than main chamber debates
* More focused, topic-driven discussions

### **`sp-new/` — Short / Special Proceedings (Commons)**

This typically includes:

* short debates
* urgent questions
* business statements
* procedural or time-limited discussions

Content is mixed and sometimes less predictable.


"""Parser for UK hansard.parliament data

Transforms HoC speeches from XML to json files.

Traverses the scrapedxml directory, parses each XML file, and extracts speeches along with metadata into scrapedjson directory in the same folder structure.
"""

def parser(source_dir=None, dest_dir=None, limit=None, single_file=None, dry_run=False):
    """Parse XML files under source_dir and write JSON files under dest_dir.

    Inputs:
    - source_dir: path to scrapedxml root (defaults to data/raw/hansard_gov_uk/scrapedxml relative to this file)
    - dest_dir: path to scrapedjson root (defaults to data/raw/hansard_gov_uk/scrapedjson relative to this file)
    - limit: optional integer to limit number of files processed (for testing)
    - single_file: optional single file path to process (absolute or relative)
    - dry_run: if True, don't write output files, just parse and report

    Outputs:
    - returns number of files processed

    Error modes: parse errors are logged and file skipped
    """
    import os
    import json
    from lxml import etree


    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    default_source = os.path.join(CURRENT_DIR, '../../../../data/raw/hansard_gov_uk/scrapedxml/')
    default_dest = os.path.join(CURRENT_DIR, '../../../../data/raw/hansard_gov_uk/scrapedjson/')

    if source_dir is None:
        source_dir = default_source
    if dest_dir is None:
        dest_dir = default_dest
    source_dir = os.path.abspath(source_dir)
    dest_dir = os.path.abspath(dest_dir)

    # collect filenames
    filenamelist = []
    def absoluteFilePaths(rootdir):
        for dirpath, _, filenames in os.walk(rootdir):
            for f in filenames:
                if f.lower().endswith('.xml'):
                    filenamelist.append(os.path.join(dirpath, f))
        return filenamelist

    if single_file:
        # allow passing a single file to process
        single_file_path = single_file
        if not os.path.isabs(single_file_path):
            single_file_path = os.path.abspath(single_file_path)
        if not os.path.exists(single_file_path):
            raise FileNotFoundError(single_file_path)
        filenamelist = [single_file_path]
    else:
        filenamelist = sorted(absoluteFilePaths(source_dir))

    total = len(filenamelist)

    def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
        if total == 0:
            return
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
        if iteration == total:
            print()

    print('Parsing from:', source_dir)
    print('Writing to  :', dest_dir)
    printProgressBar(0, total, prefix='Parsing...', suffix='Complete', length=50)

    processed = 0
    for idx, filename in enumerate(filenamelist):
        if limit is not None and processed >= int(limit):
            break

        try:
            parser = etree.XMLParser(dtd_validation=False, recover=True)
            tree = etree.parse(filename, parser)
            root = tree.getroot()
        except Exception as e:
            print(f"\nERROR parsing {filename}: {e}")
            printProgressBar(idx + 1, total, prefix='Parsing...', suffix='Complete', length=50)
            continue

        jsondoc = {
            'meta': {
                'source_file': os.path.relpath(filename, source_dir),
                'root_tag': root.tag,
                'root_attrib': dict(root.attrib)
            },
            'speeches': []
        }

        # Determine parser behavior by scanning element local names (namespace-agnostic)
        has_speech = False
        has_ques = False
        for el in root.iter():
            tag = _strip_tag(el.tag)
            if tag == 'speech':
                has_speech = True
            if tag in ('ques', 'question'):
                has_ques = True

        if has_speech and has_ques:
            print(f"\nWARNING: File {filename} contains both <speech> and <ques> elements; defaulting to <speech> processing.")

        if has_speech:
            jsondoc['speeches'] = process_speech(root)
        elif has_ques:
            jsondoc['speeches'] = process_qa(root)
        else:
            print(f"\nWARNING: No <speech> or <ques> elements found in {filename}, skipping file.")
            printProgressBar(idx + 1, total, prefix='Parsing...', suffix='Complete', length=50)
            continue

        # build output path mirroring source_dir
        relpath = os.path.relpath(filename, source_dir)
        out_rel = os.path.splitext(relpath)[0] + '.json'
        out_path = os.path.join(dest_dir, out_rel)
        out_dir = os.path.dirname(out_path)
        if not dry_run:
            os.makedirs(out_dir, exist_ok=True)
            try:
                with open(out_path, 'w', encoding='utf-8') as fh:
                    json.dump(jsondoc, fh, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"\nERROR writing {out_path}: {e}")
                printProgressBar(idx + 1, total, prefix='Parsing...', suffix='Complete', length=50)
                continue

        processed += 1
        printProgressBar(idx + 1, total, prefix='Parsing...', suffix='Complete', length=50)

    print(f"Processed {processed} file(s)")
    return processed


def _strip_tag(tag):
    """Strip namespace from an element tag and return the localname in lower case."""
    if tag is None:
        return ''
    if isinstance(tag, str) and '}' in tag:
        return tag.split('}', 1)[1].lower()
    return str(tag).lower()


def process_speech(root):
    """Process XML tree containing <speech> elements and return list of conversations.

    This is the refactored logic that was previously inline in parser():
    - collects all <speech> elements in document order
    - builds a flat_speeches list containing id, speakername, person_id, type, oral_qnum, text
    - groups flat_speeches into conversations starting at speeches whose normalized type begins with 'startquestion'
    - returns conversations list
    """
    flat_speeches = []
    for speech in root.findall('.//speech'):
        sattrib = dict(speech.attrib)
        speech_id = sattrib.get('id')
        speakername = sattrib.get('speakername') or sattrib.get('speaker')
        person_id = sattrib.get('person_id') or sattrib.get('personid') or sattrib.get('person')
        kind = sattrib.get('type')
        oral_qnum = sattrib.get('oral-qnum') or sattrib.get('oral_qnum')

        # collect paragraph texts
        para_texts = []
        for p in speech.findall('.//p'):
            try:
                p_text = ''.join(p.itertext()).strip()
            except Exception:
                p_text = (p.text or '').strip()
            if p_text:
                para_texts.append(p_text)

        full_text = '\n\n'.join(para_texts)

        flat_speeches.append({
            'id': speech_id,
            'speakername': speakername,
            'person_id': person_id,
            'type': kind,
            'oral_qnum': oral_qnum,
            'text': full_text,
        })

    # Group flat_speeches into conversations starting with type like 'Start Question'
    conversations = []
    current = None
    for sp in flat_speeches:
        t_raw = sp.get('type') or ''
        t_norm = (t_raw or '').lower().replace(' ', '')
        name = sp.get('speakername') or 'UNKNOWN'
        segment_text = sp.get('text', '')

        # treat any variant of 'start question' as starting a new conversation
        if t_norm.startswith('startquestion'):
            # close existing conversation if present
            if current is not None:
                conversations.append(current)
            # start a new conversation record
            current = {
                'start_id': sp.get('id'),
                'speakers': [],
                'text': ''
            }
            # add first segment
            current.setdefault('speakers', [])
            speakers = current.setdefault('speakers', [])
            if name not in speakers:
                speakers.append(name)
            # include the first segment (may be empty)
            current['text'] = f"{name}:{segment_text}" if segment_text else f"{name}:"
            continue

        # skip any speeches until we hit the first Start Question
        if current is None:
            continue

        # append speaker to speakers list preserving encounter order (safe mutation for static analysis)
        assert current is not None
        speakers = current.setdefault('speakers', [])
        if name not in speakers:
            speakers.append(name)

        # append this speech segment, separated by ' \p ' (coerce current text to str to satisfy type checkers)
        if segment_text:
            current['text'] = str(current.get('text', '')) + ' \\p ' + f"{name}:{segment_text}"
        else:
            current['text'] = str(current.get('text', '')) + ' \\p ' + f"{name}:"

    # append any open conversation
    if current is not None:
        conversations.append(current)

    return conversations


def process_qa(root):
    """Process XML trees that use a question/answer schema (wrans, lordswrans).

    Strategy:
    - Walk document in order and collect elements whose localname is 'ques', 'question', 'ans', or 'answer'
    - Normalize ques -> treated as Start Question, ans/answer -> treated as Answer
    - Build flat_speeches preserving order, including speakername where available and concatenated paragraph text
    - Group flat_speeches into conversations starting at Start Question entries and appending subsequent answers until next Start Question
    """
    flat_speeches = []

    for elem in root.iter():
        tag = _strip_tag(elem.tag)
        if tag in ('ques', 'question'):
            typ = 'Start Question'
        elif tag in ('ans', 'answer'):
            typ = 'Answer'
        else:
            continue

        sattrib = dict(elem.attrib)
        # many wrans files use different attribute names; try common ones
        speakername = sattrib.get('speakername') or sattrib.get('speaker') or sattrib.get('who') or sattrib.get('name')

        # collect paragraph texts within the element
        para_texts = []
        for p in elem.findall('.//p'):
            try:
                p_text = ''.join(p.itertext()).strip()
            except Exception:
                p_text = (p.text or '').strip()
            if p_text:
                para_texts.append(p_text)

        full_text = '\n\n'.join(para_texts)

        flat_speeches.append({
            'id': sattrib.get('id'),
            'speakername': speakername,
            'person_id': sattrib.get('person_id') or sattrib.get('personid') or sattrib.get('person'),
            'type': typ,
            'text': full_text,
        })

    # Group into conversations very similarly to process_speech
    conversations = []
    current = None
    for sp in flat_speeches:
        t = (sp.get('type') or '').strip()
        name = sp.get('speakername') or 'UNKNOWN'
        seg = sp.get('text', '')

        if t.lower().startswith('startquestion'):
            if current is not None:
                conversations.append(current)
            current = {
                'start_id': sp.get('id'),
                'speakers': [],
                'text': ''
            }
            current.setdefault('speakers', [])
            speakers = current.setdefault('speakers', [])
            if name not in speakers:
                speakers.append(name)
            current['text'] = f"{name}:{seg}" if seg else f"{name}:"
            continue

        if current is None:
            # ignore answers before first question
            continue

        assert current is not None
        speakers = current.setdefault('speakers', [])
        if name not in speakers:
            speakers.append(name)
        if seg:
            current['text'] = str(current.get('text', '')) + ' \\p ' + f"{name}:{seg}"
        else:
            current['text'] = str(current.get('text', '')) + ' \\p ' + f"{name}:"

    if current is not None:
        conversations.append(current)

    return conversations


if __name__ == '__main__':
    # Local configuration - edit these values as needed before running the module
    # If left as None, the parser() defaults will be used (the repository's data/raw path).
    SOURCE_DIR = None
    DEST_DIR = None
    LIMIT = None           # e.g. 10 to process only 10 files
    SINGLE_FILE = None     # e.g. '/abs/path/to/data/raw/hansard_gov_uk/scrapedxml/debates/....xml'
    DRY_RUN = False        # set True to parse but not write files
    SINGLE_FILE = "/Users/huseyinkir/workspaces/workspace1/ndl-core-data-pipeline/data/raw/hansard_gov_uk/scrapedxml/debates/debates2025-01-06b.xml"

    # Call parser using the local variables above (no CLI required)
    parser(source_dir=SOURCE_DIR, dest_dir=DEST_DIR, limit=LIMIT, single_file=SINGLE_FILE, dry_run=DRY_RUN)

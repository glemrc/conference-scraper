"""Quick import test - writes results to test_results.txt"""
import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
results = []

def test(label, fn):
    try:
        fn()
        results.append(f"OK   {label}")
    except Exception as e:
        results.append(f"FAIL {label}: {e}")

test("text_extractor.detect_pdf_links",
     lambda: __import__("text_extractor", fromlist=["detect_pdf_links"]))
test("text_extractor._normalize_dashes",
     lambda: getattr(__import__("text_extractor", fromlist=["_normalize_dashes"]), "_normalize_dashes"))
test("shallow_crawler.field_aware_crawl_needed",
     lambda: __import__("shallow_crawler", fromlist=["field_aware_crawl_needed"]))
test("browser_use_crawler imports",
     lambda: __import__("browser_use_crawler", fromlist=["extract_via_browser_use"]))
test("scraper_v2 top-level imports",
     lambda: __import__("scraper_v2"))

output = "\n".join(results)
failed = [r for r in results if r.startswith("FAIL")]

with open("test_results.txt", "w", encoding="utf-8") as f:
    f.write(output + "\n")
    f.write("\nFAILED: " + str(failed) if failed else "\nAll imports OK!")

print(output)
print()
print("FAILED: " + str(failed) if failed else "All imports OK!")

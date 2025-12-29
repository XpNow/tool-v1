import app.parse
import inspect

print("parse.py path:", app.parse.__file__)

src = inspect.getsource(app.parse)
print("Has RE_BANK_TRANSFER:", "RE_BANK_TRANSFER" in src)
print("Regex snippet contains '.?':", ".?" in src)
print("Regex snippet contains 'transferat':", "transferat" in src)

**Adding a new Exchange Calendar**

- [ ] Added new exchange to readme
- [ ] Added calendar class (python module like `exchange_calendars/exchange_calendar_{Exchange MIC}.py`)
    -  Class should contain reference to trading calendar on exchange website
- [ ] Referenced new calendar in `exchange_calendars/calendar_utils.py`
- [ ] Added tests (e.g `tests/test_{Exchange MIC}_calendar.py`)
   - You can generate tests input using `python etc/make_exchange_calendar_test_csv.py {Exchange MIC}`

**Modifying an existing Exchange Calendar**

- [ ] Modify the test resources file (e.g `tests/resources/{Exchange MIC}.csv`), either manually or executing `python etc/make_exchange_calendar_test_csv.py {Exchange MIC}`
- [ ] Check if any of the fixtrues in `tests/test_{Exchange MIC}_calendar.py` need updating to reflect your changes
- [ ] Add references to any new/modified holidasy in `exchange_calendars/exchange_calendar_{Exchange MIC}.py`.

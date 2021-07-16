from unittest import TestCase

import pandas as pd
from pytz import UTC

from exchange_calendars.exchange_calendar_xbse import XBSEExchangeCalendar

from .test_exchange_calendar import ExchangeCalendarTestBase


class XBSECalendarTestCase(ExchangeCalendarTestBase, TestCase):

    answer_key_filename = "xbse"
    calendar_class = XBSEExchangeCalendar

    # The XBSE is open from 10:00 to 5:20PM on its longest trading day
    MAX_SESSION_HOURS = 7 + (3 / 4)

    HAVE_EARLY_CLOSES = False

    DAYLIGHT_SAVINGS_DATES = ["2018-03-26", "2018-10-29"]

    def test_regular_holidays(self):
        all_sessions = self.calendar.all_sessions

        expected_holidays = [
            pd.Timestamp("2021-01-01", tz=UTC),  # New Year's Day
            pd.Timestamp("2021-04-30", tz=UTC),  # Orthodox Good Friday
            pd.Timestamp("2021-06-01", tz=UTC),  # Children's day
            pd.Timestamp("2021-06-21", tz=UTC),  # Orthodox Pentecost
            pd.Timestamp("2021-11-30", tz=UTC),  # St. Adnrew's Day
            pd.Timestamp("2021-12-01", tz=UTC),  # National Day
            pd.Timestamp("2020-01-01", tz=UTC),  # New Year's Day
            pd.Timestamp("2020-01-02", tz=UTC),  # New Year's Day
            pd.Timestamp(
                "2020-01-24", tz=UTC
            ),  # Romanian Principalities Unification Day
            pd.Timestamp("2020-04-17", tz=UTC),  # Good Friday
            pd.Timestamp("2020-04-20", tz=UTC),  # Orthodox Easter
            pd.Timestamp("2020-05-01", tz=UTC),  # Labour Day
            pd.Timestamp("2020-06-01", tz=UTC),  # Children's Day
            pd.Timestamp("2020-06-08", tz=UTC),  # Orthodox Pentecost
            pd.Timestamp("2020-11-30", tz=UTC),  # St. Adnrew's day
            pd.Timestamp("2020-12-01", tz=UTC),  # National Day
            pd.Timestamp("2020-12-25", tz=UTC),  # Christmans
            pd.Timestamp("2019-01-01", tz=UTC),  # New Year's Day
            pd.Timestamp("2019-01-02", tz=UTC),  # New Year's Day
            pd.Timestamp(
                "2019-01-24", tz=UTC
            ),  # Romanian Principalities Unification Day
            pd.Timestamp("2019-04-26", tz=UTC),  # Good Friday
            pd.Timestamp("2019-04-29", tz=UTC),  # Orthodox Easter
            pd.Timestamp("2019-05-01", tz=UTC),  # Labour Day
            pd.Timestamp("2019-06-17", tz=UTC),  # Orthodox Pentecost
            pd.Timestamp("2019-08-15", tz=UTC),  # Assumption of Virgin Mary
            pd.Timestamp("2019-12-25", tz=UTC),  # Christmans
            pd.Timestamp("2019-12-26", tz=UTC),  # Christmans
        ]

        for holiday_label in expected_holidays:
            self.assertNotIn(holiday_label, all_sessions)

    def test_holidays_fall_on_weekend(self):
        all_sessions = self.calendar.all_sessions

        # All holidays that fall on a weekend should not be made
        # up, so ensure surrounding days are open market
        expected_sessions = [
            # Second New Years Day on Saturday, Jan 2st
            pd.Timestamp("2021-01-04", tz=UTC),
            # Christmas on a Saturday Sunday
            #   Note: 25th and 26th are holidays
            pd.Timestamp("2021-12-24", tz=UTC),
            pd.Timestamp("2021-12-27", tz=UTC),
            # Labour Day on Saturday + Good Friday on Friday + Orthodox Easter on Monday
            pd.Timestamp("2021-04-29", tz=UTC),
            pd.Timestamp("2021-05-04", tz=UTC),
            # Children's Day on Saturday
            pd.Timestamp("2019-05-31", tz=UTC),
            pd.Timestamp("2019-06-03", tz=UTC),
            # Assumption of Virgin Mary on Sunday
            pd.Timestamp("2021-08-13", tz=UTC),
            pd.Timestamp("2021-08-16", tz=UTC),
            # Assumption of Virgin Mary on Saturday
            pd.Timestamp("2020-08-14", tz=UTC),
            pd.Timestamp("2020-08-17", tz=UTC),
        ]

        for session_label in expected_sessions:
            self.assertIn(session_label, all_sessions)

    def test_orthodox_easter(self):
        """
        The Athens Stock Exchange observes Orthodox (or Eastern) Easter,
        as well as Western Easter.  All holidays that are tethered to
        Easter (i.e. Whit Monday, Good Friday, etc.), are relative to
        Orthodox Easter.  This test checks that Orthodox Easter and all
        related holidays are correct.
        """
        all_sessions = self.calendar.all_sessions

        expected_holidays = [
            # Some Orthodox Good Friday dates
            pd.Timestamp("2002-05-03", tz=UTC),
            pd.Timestamp("2005-04-29", tz=UTC),
            pd.Timestamp("2008-04-25", tz=UTC),
            pd.Timestamp("2009-04-17", tz=UTC),
            pd.Timestamp("2016-04-29", tz=UTC),
            pd.Timestamp("2017-04-14", tz=UTC),
            # Some Orthodox Pentecost dates
            pd.Timestamp("2002-06-24", tz=UTC),
            pd.Timestamp("2005-06-20", tz=UTC),
            pd.Timestamp("2006-06-12", tz=UTC),
            pd.Timestamp("2008-06-16", tz=UTC),
            pd.Timestamp("2013-06-24", tz=UTC),
            pd.Timestamp("2016-06-20", tz=UTC),
        ]

        for holiday_label in expected_holidays:
            self.assertNotIn(holiday_label, all_sessions)

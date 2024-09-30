# 交易日历

日历数据来源：

- [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars)
- [Tushare](https://tushare.pro/)
- [tqsdk](https://www.shinnytech.com/)
- [chinese_calendar](https://pypi.org/project/chinesecalendar/)

## 特性

- `exchange_calendars`考虑了盘中特殊原因提前收盘，或者延迟开盘的时间，本模块没有考虑
- 支持不同证券品种生成不同交易日历，比如中国期货
- 支持查询不同周期的K线时间，支持和东方财富期货、新浪期货相同的K线时间

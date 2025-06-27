# 交易日历

依赖：

 - 下载并安装 [unordered_dense](https://github.com/martinus/unordered_dense)

关于分支：

 - `main`
    
    日历数据保存于MongoDB，运行时从Mongodb加载并初始化。所以依赖：

    下载并安装 [quantdata](https://github.com/sric0880/quantdata)，依赖其中的`qddatetime`库，和`qdmongdb`用于测试

    ```
    Windows下由于安装Mongodb路径问题，可以需要传CMAKE_PREFIX_PATH才能编译安装

    MongoDB只是在测试用例中使用，如果不测试，可以不填写`CMAKE_PREFIX_PATH`。如果需要运行测试，才需要填写`CMAKE_PREFIX_PATH`。两种方法修改`CMAKE_PREFIX_PATH`:

    1. 在Visual Studio中调试：修改`CMakePresets.json`中的configurePresets > windows-base > CMAKE_PREFIX_PATH。这个是前面安装mongo cxx driver的`CMAKE_INSTALL_PREFIX`，告诉CMake去哪个目录查找MongoDB的库文件

    2. 命令行输入-DCMAKE_PREFIX_PATH。
    ```

 - `no-depends`

    该分支不依赖`quantdata`以及mongodb库。日历数据直接导出为cpp文件，并编译成动态链接库。

日历数据来源：

- [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars)
- [Tushare](https://tushare.pro/)
- [tqsdk](https://www.shinnytech.com/)
- [chinese_calendar](https://pypi.org/project/chinesecalendar/)

日历数据导出见文件夹`tools`

详细使用接口参考测试用例`tests`

简单使用：

```python
from quantcalendar import CalendarCTP
cal = CalendarCTP("ag2512")     # "IF", "T"...
print(cal)
```

输出：

```text
时区: Asia/Shanghai
交易时间段:
        1) 21:00:00-02:30:00(+1 days)
        2) 09:00:00-10:15:00
        3) 10:30:00-11:30:00
        4) 13:30:00-15:00:00
K线时间点划分:
        1m)     [21:01:00, 21:02:00, 21:03:00, 21:04:00,...14:57:00, 14:58:00, 14:59:00, 15:00:00]
        3m)     [21:03:00, 21:06:00, 21:09:00, 21:12:00,...14:51:00, 14:54:00, 14:57:00, 15:00:00]
        5m)     [21:05:00, 21:10:00, 21:15:00, 21:20:00,...14:45:00, 14:50:00, 14:55:00, 15:00:00]
        10m)    [21:10:00, 21:20:00, 21:30:00, 21:40:00,...14:35:00, 14:45:00, 14:55:00, 15:00:00]
        15m)    [21:15:00, 21:30:00, 21:45:00, 22:00:00,...14:15:00, 14:30:00, 14:45:00, 15:00:00]
        30m)    [21:30:00, 22:00:00, 22:30:00, 23:00:00,...13:45:00, 14:15:00, 14:45:00, 15:00:00]
        1H)     [22:00:00, 23:00:00, 00:00:00, 01:00:00,...10:45:00, 13:45:00, 14:45:00, 15:00:00]
        2H)     [23:00:00,01:00:00,09:30:00,13:45:00,15:00:00]
        3H)     [00:00:00,09:30:00,14:45:00,15:00:00]
        4H)     [01:00:00,13:45:00,15:00:00]
```

## 特性

- 目前支持A股、中国期货、7x24全天候交易日历
- 支持C++、Python(Cython封装)，相比纯Python实现的版本【已删除】，性能提升5-50倍（不同接口不同）
- 支持不同证券品种生成不同交易日历，比如中国期货
- 支持集合竞价时间查询
- 支持查询不同周期的K线时间。支持和东方财富期货、新浪期货相同的K线时间
- 时间相关函数对比，timestamp_xx系列函数效率高于Python；而combine和timedelta系列函数Python效率高于Cython实现

```python
from quantcalendar import timestamp_us
from numpy import datetime64
from datetime import datetime,timezone


d1 = datetime64('2011-07-18', "s")
d3 = datetime(2011, 7, 18, tzinfo=timezone.utc)
print(timestamp_us(d1))
print(d3.timestamp())
print(timestamp_us(d3))
# 1310947200000000
# 1310947200000000
# 1310947200000000

%timeit timestamp_us(d1)
# 151 ns ± 18.5 ns per loop (mean ± std. dev. of 7 runs, 1,000,000 loops each)
%timeit int(d3.timestamp()*1000000)
# 831 ns ± 148 ns per loop (mean ± std. dev. of 7 runs, 1,000,000 loops each)
%timeit timestamp_us(d3)
# 145 ns ± 6.45 ns per loop (mean ± std. dev. of 7 runs, 10,000,000 loops each)
```

## 安装

### C++

可以通过传入 -DBUILD_TESTING=OFF禁止测试编译

#### MacOS/Linux

```sh
cmake -S . -B ./build -DCMAKE_BUILD_TYPE=Release
cmake --build ./build
sudo cmake --build ./build --target install
```

#### Windows

命令行安装：

```sh
cmake -S . -B ./build -G "Visual Studio 17 2022" -DBUILD_TESTING=OFF
cmake --build ./build --config RelWithDebInfo --parallel
cmake --build ./build --target install --config RelWithDebInfo
```

### Python

install build tools

```sh
pip install scikit-build
pip install Cython
```

#### MacOS/Linux

```sh
pip install .

# or 
python setup.py install

# build inplace
python setup.py build_ext --inplace

# build wheel
python setup.py bdist_wheel
```

#### Windows

使用`pip install`无法传参。如果不传`-DCMAKE_BUILD_TYPE=RelWithDebInfo`默认是`Release`，根据需要自己设定。

```sh
python .\setup.py install -G "Visual Studio 17 2022" -- -DCMAKE_BUILD_TYPE=RelWithDebInfo

# build inplace
python .\setup.py build_ext --inplace -G "Visual Studio 17 2022" -- -DCMAKE_BUILD_TYPE=RelWithDebInfo
```

## 测试

### C++

先编译

```sh
# 添加 -V 打印所有输出
ctest -T test --test-dir out/build/linux-debug --output-on-failure
```

### Python

先原地构建，再运行

```sh
python -m pytest tests
```

## 在其他项目作为依赖库使用

在其他项目的CMakeLists.txt中添加

```cmake
find_package(quantcalendar REQUIRED)
target_link_libraries(YOUR_LIB quantcalenar::quantcalendar)
```

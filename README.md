# 交易日历

日历数据来源：

- [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars)
- [Tushare](https://tushare.pro/)
- [tqsdk](https://www.shinnytech.com/)
- [chinese_calendar](https://pypi.org/project/chinesecalendar/)

***数据需自行整理导入数据库***

本库测试使用MongoDB。使用接口参考测试用例`tests`

## 特性

- 目前支持A股、中国期货、7x24全天候交易日历
- 支持C++、Python(Cython封装)，相比纯Python实现的版本【已删除】，性能提升5-50倍（不同接口不同）
- 支持不同证券品种生成不同交易日历，比如中国期货
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

### 依赖项

2. 下载并安装 [unordered_dense](https://github.com/martinus/unordered_dense)

### C++

#### MacOS/Linux

```sh
cmake -S . -B ./build -DCMAKE_BUILD_TYPE=Release
cmake --build ./build
sudo cmake --build ./build --target install
```

#### Windows

MongoDB只是在测试用例中使用，如果不测试，可以不填写`CMAKE_PREFIX_PATH`，可以通过传入 -DBUILD_TESTING=OFF禁止测试编译。
如果需要运行测试，才需要填写`CMAKE_PREFIX_PATH`。两种方法修改`CMAKE_PREFIX_PATH`:

- 在Visual Studio中调试：修改`CMakePresets.json`中的configurePresets > windows-base > CMAKE_PREFIX_PATH。这个是前面安装mongo cxx driver的`CMAKE_INSTALL_PREFIX`，告诉CMake去哪个目录查找MongoDB的库文件。
- 命令行输入-DCMAKE_PREFIX_PATH。

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

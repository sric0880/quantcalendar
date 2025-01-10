# 交易日历

日历数据来源：

- [exchange_calendars](https://github.com/gerrymanoim/exchange_calendars)
- [Tushare](https://tushare.pro/)
- [tqsdk](https://www.shinnytech.com/)
- [chinese_calendar](https://pypi.org/project/chinesecalendar/)

数据需自行导入数据库。本库测试使用MongoDB。

## 特性

- 支持Python和C++(正在开发中...)两种接口
- 支持不同证券品种生成不同交易日历，比如中国期货
- 支持查询不同周期的K线时间，支持和东方财富期货、新浪期货相同的K线时间

## 安装

### 依赖项

1. 下载并安装 [quantdata](https://github.com/sric0880/quantdata)，依赖其中的`qddatetime`库，和`qdmongdb`用于测试
2. 下载并安装 [unordered_dense](https://github.com/martinus/unordered_dense)

### C++

#### MacOS/Linux

```sh
cmake -S . -B ./build -DCMAKE_BUILD_TYPE=Release
cmake --build ./build
sudo cmake --build ./build --target install
```

#### Windows

在Visual Studio中调试：修改`CMakePresets.json`中的configurePresets > windows-base > CMAKE_PREFIX_PATH。这个是前面安装mongo cxx driver的`CMAKE_INSTALL_PREFIX`，告诉CMake去哪个目录查找MongoDB的库文件。

命令行安装：DCMAKE_PREFIX_PATH 改成自己的目录。MongoDB只是在测试用例中使用，如果不测试，可以不填写`DCMAKE_PREFIX_PATH`，并注释掉CMakeLists.txt中相关测试用例。

```sh
cmake -S . -B ./build -G "Visual Studio 17 2022" -DCMAKE_PREFIX_PATH=C:\"Program Files (x86)"\mongo-c-driver
cmake --build ./build --config RelWithDebInfo --parallel
cmake --build ./build --target install --config RelWithDebInfo
```

### Python

install build tools

```sh
pip install scikit-build
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

使用方法见测试用例

### C++

```sh

```

### Python

先原地构建，再运行

```sh
python -m pytest tests
```


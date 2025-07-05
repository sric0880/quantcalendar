from skbuild import setup

setup(
    name="quantcalendar",
    version="2025.06",
    author="lzq",
    description="trade calendar",
    license="MIT",
    packages=["quantcalendar"],
    python_requires=">=3.9",
    cmake_languages=("CXX",),
)

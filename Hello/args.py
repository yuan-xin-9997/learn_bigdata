import sys

# 通过sys.argv获取命令行参数，其中sys.argv[0]是脚本名称本身
# 剩下的参数（如果有）将被存储在sys.argv[1:]中
args = sys.argv[1:]

# 打印命令行参数
for arg in args:
    print(arg)

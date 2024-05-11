## 1.Git是干嘛？

理解VCS

## 2.Git命令在哪里使用？前提？

只能在Git仓库使用。 提前配置用户和邮箱。

在Git bash中随时随地使用，如果windows的cmd窗口，需要绝对路径(并不在windows的path中)

## 3.使用Git的原则

在工作区的所有写操作都要提交到git的本地库进行记录。

git status : 看到 nothing to commit ,working tree clean

## 4.Git要掌握的内容(了解)

![image-20220702083314946](C:\Users\86176\AppData\Roaming\Typora\typora-user-images\image-20220702083314946.png)

![image-20220702083324220](C:\Users\86176\AppData\Roaming\Typora\typora-user-images\image-20220702083324220.png)

![image-20220702083332833](C:\Users\86176\AppData\Roaming\Typora\typora-user-images\image-20220702083332833.png)



## 5.IDEA的所有Git操作(重点)



## 6.版本回退

git reflog : 查看每一次操作记录

git reset --hard 版本号(前7位)



## 7.忽略文件

在git仓库中放一个 .gitingnore文件，将不希望Git仓库管理的文件，输入进去。

.gitignore怎么写?

​			百度



## 8.理解分支



## 9.分支操作(重点)

IDEA操作必须会

git branch -v 

git checkout 分支

git branch -d 分支

git merge 分支



## 10.处理冲突(重点)

删除标记。

讨论决定最终版本。

提交冲突解决到本地库。



## 11.推送远超须知(重点)

本地必须比远超仓库的最新版本新，才有资格推送。如果落后，需要先pull,再push。

IDEA操作必须会。

clone

push

pull 



## 12.自己注册Gitee账号
/*
第12章 报表数据导出
为方便报表应用使用数据，需将ads各指标的统计结果导出到MySQL数据库中。
为什么需要导出数据到MySQL：
1.使用数据的应用，可能是由多种语言开发，比如Java、Python、C++、JavaScript，部分语言不支持读取Hive，因为将Hive数据导出到更通用的MySQL中，便于大部分应用读取
2.Hive不是基于OLTP设计，而是基于OLAP，天然的分析型数据库，查询相对较慢，体验很差，导出到MySQL便于查询
12.1 MySQL建库建表
建表和导入数据的注意事项：

粒度：描述的事一行代表一个唯一的xxx，粒度的前提是唯一
主键：保证唯一性

12.1.1 创建数据库
CREATE DATABASE IF NOT EXISTS gmall_report DEFAULT CHARSET utf8 COLLATE utf8_general_ci;
12.1.2 创建表
*/
# use gmall_report;
# DROP TABLE IF EXISTS `ads_traffic_stats_by_channel`;
# CREATE TABLE `ads_traffic_stats_by_channel`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
#   `channel` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '渠道',
#   `uv_count` bigint(20) NULL DEFAULT NULL COMMENT '访客人数',
#   `avg_duration_sec` bigint(20) NULL DEFAULT NULL COMMENT '会话平均停留时长，单位为秒',
#   `avg_page_count` bigint(20) NULL DEFAULT NULL COMMENT '会话平均浏览页面数',
#   `sv_count` bigint(20) NULL DEFAULT NULL COMMENT '会话数',
#   `bounce_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '跳出率',
#   PRIMARY KEY (`dt`, `recent_days`, `channel`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各渠道流量统计' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_page_path`;
# CREATE TABLE `ads_page_path`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `source` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转起始页面ID',
#   `target` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转终到页面ID',
#   `path_count` bigint(20) NULL DEFAULT NULL COMMENT '跳转次数',
#   PRIMARY KEY (`dt`, `source`, `target`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '页面浏览路径分析' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_user_change`;
# CREATE TABLE `ads_user_change`  (
#   `dt` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '统计日期',
#   `user_churn_count` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '流失用户数',
#   `user_back_count` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '回流用户数',
#   PRIMARY KEY (`dt`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户变动统计' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_user_retention`;
# CREATE TABLE `ads_user_retention`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `create_date` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '用户新增日期',
#   `retention_day` int(20) NOT NULL COMMENT '截至当前日期留存天数',
#   `retention_count` bigint(20) NULL DEFAULT NULL COMMENT '留存用户数量',
#   `new_user_count` bigint(20) NULL DEFAULT NULL COMMENT '新增用户数量',
#   `retention_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '留存率',
#   PRIMARY KEY (`dt`, `create_date`, `retention_day`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '留存率' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_user_stats`;
# CREATE TABLE `ads_user_stats`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
#   `new_user_count` bigint(20) NULL DEFAULT NULL COMMENT '新增用户数',
#   `active_user_count` bigint(20) NULL DEFAULT NULL COMMENT '活跃用户数',
#   PRIMARY KEY (`dt`, `recent_days`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户新增活跃统计' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_user_action`;
# CREATE TABLE `ads_user_action`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `home_count` bigint(20) NULL DEFAULT NULL COMMENT '浏览首页人数',
#   `good_detail_count` bigint(20) NULL DEFAULT NULL COMMENT '浏览商品详情页人数',
#   `cart_count` bigint(20) NULL DEFAULT NULL COMMENT '加入购物车人数',
#   `order_count` bigint(20) NULL DEFAULT NULL COMMENT '下单人数',
#   `payment_count` bigint(20) NULL DEFAULT NULL COMMENT '支付人数',
#   PRIMARY KEY (`dt`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '漏斗分析' ROW_FORMAT = DYNAMIC;
# DROP TABLE IF EXISTS `ads_new_order_user_stats`;
# CREATE TABLE `ads_new_order_user_stats`  (
#   `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `recent_days` bigint(20) NOT NULL,
#   `new_order_user_count` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`recent_days`, `dt`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# DROP TABLE IF EXISTS `ads_order_continuously_user_count`;
# CREATE TABLE `ads_order_continuously_user_count`  (
#   `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `recent_days` bigint(20) NOT NULL,
#   `order_continuously_user_count` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`dt`, `recent_days`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# DROP TABLE IF EXISTS `ads_repeat_purchase_by_tm`;
# CREATE TABLE `ads_repeat_purchase_by_tm`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近天数,30:最近30天',
#   `tm_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '品牌ID',
#   `tm_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '品牌名称',
#   `order_repeat_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '复购率',
#   PRIMARY KEY (`dt`, `recent_days`, `tm_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各品牌复购率统计' ROW_FORMAT = DYNAMIC;
#
# DROP TABLE IF EXISTS `ads_order_stats_by_tm`;
# CREATE TABLE `ads_order_stats_by_tm`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
#   `tm_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '品牌ID',
#   `tm_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '品牌名称',
#   `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
#   `order_user_count` bigint(20) NULL DEFAULT NULL COMMENT '订单人数',
#   PRIMARY KEY (`dt`, `recent_days`, `tm_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各品牌商品交易统计' ROW_FORMAT = DYNAMIC;
#
# DROP TABLE IF EXISTS `ads_order_stats_by_cate`;
# CREATE TABLE `ads_order_stats_by_cate`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
#   `category1_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级分类id',
#   `category1_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一级分类名称',
#   `category2_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '二级分类id',
#   `category2_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '二级分类名称',
#   `category3_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级分类id',
#   `category3_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '三级分类名称',
#   `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
#   `order_user_count` bigint(20) NULL DEFAULT NULL COMMENT '订单人数',
#   PRIMARY KEY (`dt`, `recent_days`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各分类商品交易统计' ROW_FORMAT = DYNAMIC;
#
# DROP TABLE IF EXISTS `ads_sku_cart_num_top3_by_cate`;
# CREATE TABLE `ads_sku_cart_num_top3_by_cate`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `category1_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级分类ID',
#   `category1_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一级分类名称',
#   `category2_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '二级分类ID',
#   `category2_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '二级分类名称',
#   `category3_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级分类ID',
#   `category3_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '三级分类名称',
#   `sku_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '商品id',
#   `sku_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '商品名称',
#   `cart_num` bigint(20) NULL DEFAULT NULL COMMENT '购物车中商品数量',
#   `rk` bigint(20) NULL DEFAULT NULL COMMENT '排名',
#   PRIMARY KEY (`dt`, `sku_id`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各分类商品购物车存量Top10' ROW_FORMAT = DYNAMIC;
#
# DROP TABLE IF EXISTS `ads_sku_favor_count_top3_by_tm`;
# CREATE TABLE `ads_sku_favor_count_top3_by_tm`  (
#   `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `tm_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `tm_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `sku_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `sku_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
#   `favor_count` bigint(20) NULL DEFAULT NULL,
#   `rk` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`dt`, `tm_id`, `sku_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#
# DROP TABLE IF EXISTS `ads_order_to_pay_interval_avg`;
# CREATE TABLE `ads_order_to_pay_interval_avg`  (
#   `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `order_to_pay_interval_avg` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`dt`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
#
# DROP TABLE IF EXISTS `ads_order_by_province`;
# CREATE TABLE `ads_order_by_province`  (
#   `dt` date NOT NULL COMMENT '统计日期',
#   `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
#   `province_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '省份ID',
#   `province_name` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '省份名称',
#   `area_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '地区编码',
#   `iso_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
#   `iso_code_3166_2` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
#   `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
#   `order_total_amount` decimal(16, 2) NULL DEFAULT NULL COMMENT '订单金额',
#   PRIMARY KEY (`dt`, `recent_days`, `province_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各地区订单统计' ROW_FORMAT = DYNAMIC;
#
# DROP TABLE IF EXISTS `ads_coupon_stats`;
# CREATE TABLE `ads_coupon_stats`  (
#   `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `coupon_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `coupon_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
#   `used_count` bigint(20) NULL DEFAULT NULL,
#   `userd_user_count` bigint(20) NULL DEFAULT NULL,
#   PRIMARY KEY (`dt`, `coupon_id`) USING BTREE
# ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;



# 1）各渠道流量统计
DROP TABLE IF EXISTS `ads_traffic_stats_by_channel`;
CREATE TABLE `ads_traffic_stats_by_channel`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `channel` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '渠道',
  `uv_count` bigint(20) NULL DEFAULT NULL COMMENT '访客人数',
  `avg_duration_sec` bigint(20) NULL DEFAULT NULL COMMENT '会话平均停留时长，单位为秒',
  `avg_page_count` bigint(20) NULL DEFAULT NULL COMMENT '会话平均浏览页面数',
  `sv_count` bigint(20) NULL DEFAULT NULL COMMENT '会话数',
  `bounce_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '跳出率',
  PRIMARY KEY (`dt`, `recent_days`, `channel`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各渠道流量统计' ROW_FORMAT = DYNAMIC;
# 2）路径分析
DROP TABLE IF EXISTS `ads_page_path`;
CREATE TABLE `ads_page_path`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `source` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转起始页面ID',
  `target` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '跳转终到页面ID',
  `path_count` bigint(20) NULL DEFAULT NULL COMMENT '跳转次数',
  PRIMARY KEY (`dt`, `source`, `target`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '页面浏览路径分析' ROW_FORMAT = DYNAMIC;
# 3）用户变动统计
DROP TABLE IF EXISTS `ads_user_change`;
CREATE TABLE `ads_user_change`  (
  `dt` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '统计日期',
  `user_churn_count` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '流失用户数',
  `user_back_count` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '回流用户数',
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户变动统计' ROW_FORMAT = DYNAMIC;
# 4）用户留存率
DROP TABLE IF EXISTS `ads_user_retention`;
CREATE TABLE `ads_user_retention`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `create_date` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '用户新增日期',
  `retention_day` int(20) NOT NULL COMMENT '截至当前日期留存天数',
  `retention_count` bigint(20) NULL DEFAULT NULL COMMENT '留存用户数量',
  `new_user_count` bigint(20) NULL DEFAULT NULL COMMENT '新增用户数量',
  `retention_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '留存率',
  PRIMARY KEY (`dt`, `create_date`, `retention_day`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '留存率' ROW_FORMAT = DYNAMIC;
# 5）用户新增活跃统计
DROP TABLE IF EXISTS `ads_user_stats`;
CREATE TABLE `ads_user_stats`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近n日,1:最近1日,7:最近7日,30:最近30日',
  `new_user_count` bigint(20) NULL DEFAULT NULL COMMENT '新增用户数',
  `active_user_count` bigint(20) NULL DEFAULT NULL COMMENT '活跃用户数',
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '用户新增活跃统计' ROW_FORMAT = DYNAMIC;
# 6）用户行为漏斗分析
DROP TABLE IF EXISTS `ads_user_action`;
CREATE TABLE `ads_user_action`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `home_count` bigint(20) NULL DEFAULT NULL COMMENT '浏览首页人数',
  `good_detail_count` bigint(20) NULL DEFAULT NULL COMMENT '浏览商品详情页人数',
  `cart_count` bigint(20) NULL DEFAULT NULL COMMENT '加入购物车人数',
  `order_count` bigint(20) NULL DEFAULT NULL COMMENT '下单人数',
  `payment_count` bigint(20) NULL DEFAULT NULL COMMENT '支付人数',
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '漏斗分析' ROW_FORMAT = DYNAMIC;
# 7）新增下单用户统计
DROP TABLE IF EXISTS `ads_new_order_user_stats`;
CREATE TABLE `ads_new_order_user_stats`  (
  `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `recent_days` bigint(20) NOT NULL,
  `new_order_user_count` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`recent_days`, `dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# 8）最近7日内连续3日下单用户数
DROP TABLE IF EXISTS `ads_order_continuously_user_count`;
CREATE TABLE `ads_order_continuously_user_count`  (
  `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `recent_days` bigint(20) NOT NULL,
  `order_continuously_user_count` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`, `recent_days`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# 9）最近30日各品牌复购率
DROP TABLE IF EXISTS `ads_repeat_purchase_by_tm`;
CREATE TABLE `ads_repeat_purchase_by_tm`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近天数,30:最近30天',
  `tm_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '品牌ID',
  `tm_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '品牌名称',
  `order_repeat_rate` decimal(16, 2) NULL DEFAULT NULL COMMENT '复购率',
  PRIMARY KEY (`dt`, `recent_days`, `tm_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各品牌复购率统计' ROW_FORMAT = DYNAMIC;
# 10）各品牌商品下单统计
DROP TABLE IF EXISTS `ads_order_stats_by_tm`;
CREATE TABLE `ads_order_stats_by_tm`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `tm_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '品牌ID',
  `tm_name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '品牌名称',
  `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
  `order_user_count` bigint(20) NULL DEFAULT NULL COMMENT '订单人数',
  PRIMARY KEY (`dt`, `recent_days`, `tm_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各品牌商品交易统计' ROW_FORMAT = DYNAMIC;
# 11）各分类商品下单统计
DROP TABLE IF EXISTS `ads_order_stats_by_cate`;
CREATE TABLE `ads_order_stats_by_cate`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `category1_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级分类id',
  `category1_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一级分类名称',
  `category2_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '二级分类id',
  `category2_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '二级分类名称',
  `category3_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级分类id',
  `category3_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '三级分类名称',
  `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
  `order_user_count` bigint(20) NULL DEFAULT NULL COMMENT '订单人数',
  PRIMARY KEY (`dt`, `recent_days`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各分类商品交易统计' ROW_FORMAT = DYNAMIC;
# 12）各分类商品购物车存量Top3
DROP TABLE IF EXISTS `ads_sku_cart_num_top3_by_cate`;
CREATE TABLE `ads_sku_cart_num_top3_by_cate`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `category1_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '一级分类ID',
  `category1_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '一级分类名称',
  `category2_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '二级分类ID',
  `category2_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '二级分类名称',
  `category3_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '三级分类ID',
  `category3_name` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '三级分类名称',
  `sku_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '商品id',
  `sku_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '商品名称',
  `cart_num` bigint(20) NULL DEFAULT NULL COMMENT '购物车中商品数量',
  `rk` bigint(20) NULL DEFAULT NULL COMMENT '排名',
  PRIMARY KEY (`dt`, `sku_id`, `category1_id`, `category2_id`, `category3_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各分类商品购物车存量Top10' ROW_FORMAT = DYNAMIC;
# 13）各品牌商品收藏次数Top3
DROP TABLE IF EXISTS `ads_sku_favor_count_top3_by_tm`;
CREATE TABLE `ads_sku_favor_count_top3_by_tm`  (
  `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tm_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tm_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `sku_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `sku_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `favor_count` bigint(20) NULL DEFAULT NULL,
  `rk` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`, `tm_id`, `sku_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# 14）下单到支付时间间隔平均值
DROP TABLE IF EXISTS `ads_order_to_pay_interval_avg`;
CREATE TABLE `ads_order_to_pay_interval_avg`  (
  `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `order_to_pay_interval_avg` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
# 15）各省份交易统计
DROP TABLE IF EXISTS `ads_order_by_province`;
CREATE TABLE `ads_order_by_province`  (
  `dt` date NOT NULL COMMENT '统计日期',
  `recent_days` bigint(20) NOT NULL COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
  `province_id` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '省份ID',
  `province_name` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '省份名称',
  `area_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '地区编码',
  `iso_code` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
  `iso_code_3166_2` varchar(16) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '国际标准地区编码',
  `order_count` bigint(20) NULL DEFAULT NULL COMMENT '订单数',
  `order_total_amount` decimal(16, 2) NULL DEFAULT NULL COMMENT '订单金额',
  PRIMARY KEY (`dt`, `recent_days`, `province_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '各地区订单统计' ROW_FORMAT = DYNAMIC;
# 16）优惠券使用情况统计
DROP TABLE IF EXISTS `ads_coupon_stats`;
CREATE TABLE `ads_coupon_stats`  (
  `dt` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `coupon_id` varchar(20) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `coupon_name` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `used_count` bigint(20) NULL DEFAULT NULL,
  `userd_user_count` bigint(20) NULL DEFAULT NULL,
  PRIMARY KEY (`dt`, `coupon_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
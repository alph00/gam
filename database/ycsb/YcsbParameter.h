#ifndef __DATABASE_YCSB_PARAMETER_H__
#define __DATABASE_YCSB_PARAMETER_H__

// initial data populate 暂时这样设置
#define YCSB_POPULATE_NODE_TYPE \
    0  // 由几个节点生成数据  0代表平均分配，1代表集中在某一个节点
#define YCSB_POPULATE_NODE_ID \
    0  // 当只有一个节点生成数据时，由哪个节点生成数据 // 0：158 ;  1:159 ;
       // 2:160,按照config.txt上的顺序以此类推

//  data parameter
#define YCSB_TABLE_LENTH 39999  // 表的大小
#define YCSB_TABLE_F_INT_SIZE \
    4  // 更改这里时还要注意更改YcsbRecords.h里F0的类型
#define YCSB_TABLE_F_STRING_SIZE \
    32  // string size为55是record<512条件下的最大值
#define YCSB_TABLE_ITEMCOUNT 10  // 表的项数
// record size=4+9*string size+1+8

// #define YCSB_TABLE_F_INT_MIN 1
// #define YCSB_TABLE_F_INT_MAX (YCSB_TABLE_LENTH - 1)

// txn parameter
#define YCSB_TXN_LEHTH 16  // 一个事务有16个读/写

// ycsb parameter
// 这里是默认值，其值由experiment-ycsb.sh中决定
#define YCSB_GET_RATIO 0.9                                       // 0.9
#define YCSB_UPDATE_RATIO (1 - YCSB_GET_RATIO - YCSB_PUT_RATIO)  // 0.1
#define YCSB_PUT_RATIO 0

#define UNIFORM
#define ZIPFIAN true
#define THETA 0.9  // 这里是默认值，其值由experiment-ycsb.sh中决定

#endif

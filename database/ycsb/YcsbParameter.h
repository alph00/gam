#ifndef __DATABASE_YCSB_PARAMETER_H__
#define __DATABASE_YCSB_PARAMETER_H__

// initial data populate 暂时这样设置
#define YCSB_POPULATE_NODE_NUM 3 // 由几个节点生成数据  1或3
#define YCSB_POPULATE_NODE_ID                                                  \
  0 // 当只有一个节点生成数据时，由哪个节点生成数据 // 0：158 ;  1:159 ;  2:160

//  data parameter
#define YCSB_TABLE_LENTH 39999 // 表的大小 // 若三个节点，此处应设置为3的倍数
#define YCSB_TABLE_F_INT_SIZE 32
#define YCSB_TABLE_F_STRING_SIZE 32
#define YCSB_TABLE_F_INT_MIN 1
#define YCSB_TABLE_F_INT_MAX 40000

// txn parameter
#define YCSB_TXN_LEHTH 16 // 一个事务有16个读/写

// ycsb parameter
#define YCSB_GET_RATIO 0.9                                      // 0.9
#define YCSB_UPDATE_RATIO (1 - YCSB_GET_RATIO - YCSB_PUT_RATIO) // 0.1
#define YCSB_PUT_RATIO 0

#define UNIFORM
#define ZIPFIAN true
#define THETA 0.9 // 这里是默认值，其值在experiment-ycsb.sh中最终确定

#endif

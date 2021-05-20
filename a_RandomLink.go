package main

import (
   "bytes"
   "crypto/sha256"
   "fmt"
   "sync"
   "strconv"
   "time"
   "math/big"
   "math"
   "encoding/gob"
   "github.com/boltdb/bolt"
   //"flag"
   "os"
   "log"
   "crypto/ecdsa"
   "encoding/hex"
   "errors"
   "crypto/elliptic"
   //"math/rand"
   "crypto/rand"
   "golang.org/x/crypto/ripemd160"
)

var nodeSet []*Node
var chanSet [][]chan Node
var sigSet [][]chan bool
var Group []int

func main() {
   var wait sync.WaitGroup
   wait.Add(Num_Basic + Num_Attacker + 1 + 1)
   // Center Node + Basic Node + Attacker Node + System
   
   //Time_Start := time.Now().UnixNano()

   //node := []*Node{}
   //0: Center Node, 1~Nb: Basic, Nb+1~Na: Attacker

   for from := 0; from <= Num_Basic + Num_Attacker; from++ {
      var tmpSet []chan Node
      for to := 0; to <= Num_Basic + Num_Attacker; to++ {
         tmp := make(chan Node, 1)
         tmpSet = append(tmpSet, tmp)
      }
      chanSet = append(chanSet, tmpSet)
   }
   // set up for channel_node

   for from := 0; from <= Num_Basic + Num_Attacker; from++ {
      var tmpSet []chan bool
      for to := 0; to <= Num_Basic + Num_Attacker; to++ {
         tmp := make(chan bool, 1)
         tmpSet = append(tmpSet, tmp)
      }
      sigSet = append(sigSet, tmpSet)
   }
   // set up for channel_bool

   for idx := 0; idx <= Num_Basic + Num_Attacker; idx ++ {
      nd := NewNode(idx)
      /*for to := 0; to <= Num_Basic + Num_Attacker ; to ++ {
         if to != idx {
            nd.link = append(nd.link, to)
         }
      }*/
      nodeSet = append(nodeSet, nd)

      var base int
      base = 0
      Group = append(Group, base)
   }
   // set up for node

   for idx := 0; idx <= (Num_Basic + Num_Attacker)/10; idx++ {
      nodeSet[idx*10].Donation()
   }
   
   for idx := 0; idx < (Num_Basic + Num_Attacker)/10; idx++ {
      nodeSet[idx*10].BasicUpdate()
   }

   // giving a base coin

   BlockCnt = 0
   TransactionCnt = 0
   Attack_S = 0
   Attack_F = 0
   Share_Now = false
   // set up for variable

   for _, nd := range nodeSet {
      fmt.Print(nd.ID)
      fmt.Print(": ")
      if nd.center {
         fmt.Println("center Node")
      } else if nd.attacker {
         fmt.Println("attacker Node")
      } else {
         fmt.Println("basic Node")
      }
   }
   //print the type of node

   Start_Time = time.Now().UnixNano()
   Cnt_Reboot = 0
   go System(&wait)
   time.Sleep(time.Second)
   for _, nd := range nodeSet {
      go nd.Run(&wait)
   }

   wait.Wait()
   End_Time = time.Now().UnixNano()
   fmt.Printf("Experiment End\n")
      
   fmt.Printf("%d: %d %d\n", End_Time-Start_Time, TransactionCnt, BlockCnt)

   for i := 0; i <= Num_Basic + Num_Attacker; i++ {      
      fmt.Printf("Node %d has %d BTC\n", i, nodeSet[i].GetBalance())
   }

   //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
   //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))
}

const version = byte(0x00)

//var Current_Time int64
var Start_Time int64
var End_Time int64
const Period_Reboot = 5000
const Time_Delay = 500
const Time_Share = 2000
var Share_Now bool

var Cnt_Reboot int
const Limit_Reboot = 3

const Num_Basic = 30
const Num_Attacker = 0

const Num_LinkIn = 5;
const Num_LinkOut = 5;

const dbFile = "blockchain_%d.db"
const mpFile = "mempool_%d.db"
const blockBucket = "b"
const utxoBucket = "u"
const mempoolBucket = "m"

const targetBits = 16

const MinTx = 1

const targetNode = 1

var Attack_S int64
var Attack_F int64

var BlockCnt int64
var TransactionCnt int64

func System(wait *sync.WaitGroup) {
   defer wait.Done()
   for Cnt_Reboot <= Limit_Reboot {
      fmt.Printf("Reboot: %d\n", Cnt_Reboot)
      time.Sleep(Period_Reboot*time.Millisecond)

      attackSuccess := true
      for _, val := range nodeSet[targetNode].link {
         if val <= Num_Basic {
            attackSuccess = false
         }
      }
      if Cnt_Reboot > 0 && attackSuccess {
         Attack_S++
      } else {
         Attack_F++
      }

      Share_Now = true
      time.Sleep(Time_Share*time.Millisecond)
      Share_Now = false

      Cnt_Reboot ++ 
      for i := 0; i <= Num_Basic + Num_Attacker; i++ {
         nodeSet[i].link = []int{}
         /*if nodeSet[i].blockchain != nil {
            fmt.Printf("! Node %d has %d BTC / %d \n", i, nodeSet[i].GetBalance(), nodeSet[i].blockchain.Length)
         }*/
      }
   }
   fmt.Printf("Last Reboot\n")
}

type Node struct {
   ID int
   blockchain *Blockchain
   utxoSet UTXOSet
   wallet *Wallet
   mempool string
   link []int
   forUpdate []int
   center bool
   attacker bool
   active bool
   in int
   out int
}

func CreateMempool(file string) {
   db, err := bolt.Open(file, 0600, nil)
   if err != nil {
      fmt.Println("CreateMempool_1")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      _, err = tx.CreateBucketIfNotExists([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("CreateMempool_2")
         log.Panic(err)
      }

      err = tx.DeleteBucket([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("CreateMempool_3")
         log.Panic(err)
      }

      _, err = tx.CreateBucket([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("CreateMempool_4")
         log.Panic(err)
      }
      return nil
   })
   if err != nil {
      fmt.Println("CreateMempool_5")
      log.Panic(err)
   }

   db.Close()
}

func (node *Node) UpdateMempool(from string) {
   db, err := bolt.Open(node.mempool, 0600, nil)
   if err != nil {
      fmt.Println("UpdateMempool_1")
      log.Panic(err)
   }
   db0, err := bolt.Open(from, 0600, nil)
   if err != nil {
      fmt.Println("UpdateMempool_2")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      _, err = tx.CreateBucketIfNotExists([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("CreateMempool_3")
         log.Panic(err)
      }

      err = tx.DeleteBucket([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("UpdateMempool_4")
         log.Panic(err)
      }

      bucket, err := tx.CreateBucket([]byte(mempoolBucket))
      if err != nil {
         fmt.Println("UpdateMempool_5")
         log.Panic(err)
      }

      err = db0.View(func(tx0 *bolt.Tx) error {
         bucket0 := tx0.Bucket([]byte(mempoolBucket))
         c0 := bucket0.Cursor()

         for k, v := c0.First(); k != nil; k, v = c0.Next() {
            err = bucket.Put(k, v)
            if err != nil {
               fmt.Println("UpdateMempool_6")
               log.Panic(err)
            }
         }
         return nil
      })
      if err != nil {
         fmt.Println("UpdateMempool_7")
         log.Panic(err)
      }

      return nil
   })
   if err != nil {
      fmt.Println("UpdateMempool_8")
      log.Panic(err)
   }

   db0.Close()
   db.Close()
}

func NewNode(index int) *Node {
   mp := fmt.Sprintf(mpFile, index)
   returnNode := &Node{index, nil, UTXOSet{nil}, nil, mp, nil, []int{}, false, false, false, 0, 0}
   CreateMempool(returnNode.mempool)
   returnNode.wallet = NewWallet()
   if index%10 == 0 {
      returnNode.center = true
      returnNode.wallet = NewWallet()
      returnNode.active = true
      /*if index == 0 {
         returnNode.blockchain = CreateBlockchain(address, returnNode.ID)
         for i := 1; i <= (Num_Basic+Num_Attacker)/10; i++ {
            returnNode.Send(i*10, subsidy + (subsidy/2)*9)
         }
      } else {
         returnNode.blockchain = CopyBlockchain(returnNode.ID, 0, nodeSet[0].blockchain.Length)
      }
      returnNode.utxoSet = UTXOSet{returnNode.blockchain}
      returnNode.utxoSet.Reindex()*/
   }
   if index > Num_Basic {
      returnNode.attacker = true
   }
   return returnNode
}

func (node *Node) Donation() {
   if node.ID == 0 {
      address := string(node.wallet.GetAddress())
      node.blockchain = CreateBlockchain(address, node.ID)
      node.utxoSet = UTXOSet{node.blockchain}
      node.utxoSet.Reindex()
      for i := 1; i <= (Num_Basic+Num_Attacker)/10; i++ {
         node.Send(i*10, subsidy + (subsidy/2)*9)
         node.MiningBlock()
      }
   } else {
      node.blockchain = CopyBlockchain(node.ID, (node.ID/10-1)*10, nodeSet[(node.ID/10-1)*10].blockchain.Length)
      node.utxoSet = UTXOSet{node.blockchain}
      node.utxoSet.Reindex()
      for i := 1; i < 10; i++ {
         node.Send(node.ID-i, subsidy/2)
         node.MiningBlock()
      }
   }
}

func (node *Node) BasicUpdate() {
   node.blockchain = CopyBlockchain(node.ID, Num_Basic + Num_Attacker, nodeSet[Num_Basic + Num_Attacker].blockchain.Length)
   node.utxoSet = UTXOSet{node.blockchain}
   node.utxoSet.Reindex()
}

func (node *Node) Run(wait *sync.WaitGroup) {
   defer wait.Done()
   for Cnt_Reboot <= Limit_Reboot {
      End_Time = time.Now().UnixNano()
      //fmt.Printf("%d: %d %d / ", End_Time-Start_Time, TransactionCnt, BlockCnt)
      //fmt.Printf("%d %d\n", Attack_S, Attack_S+Attack_F)

      RandomValue, err := rand.Int(rand.Reader, big.NewInt(10))
      if err != nil {
         fmt.Println("Run_1")
         log.Panic(err)
      }
      Switch := *RandomValue
      CompareOn := big.NewInt(9)
      CompareOff := big.NewInt(0)

      if node.ID % 10 == 0 && node.ID != 0{
         for i := 1; i < 10; i++ {
            if len(sigSet[node.ID-i][node.ID]) > 0 {
               <-sigSet[node.ID-i][node.ID]

               //node.Send(node.ID-i, subsidy/2)

               idx := ((Num_Basic + Num_Attacker + 1)*(node.ID + 1) + node.ID - i)   
               mp := fmt.Sprintf(mpFile, idx)
               tmpNd := Node{idx, nil, UTXOSet{nil}, nil, mp, nil, []int{}, false, false, false, 0, 0}
               tmpNd.blockchain = CopyBlockchain(idx, node.ID, node.blockchain.Length)
               tmpNd.utxoSet = UTXOSet{node.blockchain}
               tmpNd.utxoSet.Reindex()
               tmpNd.UpdateMempool(node.mempool)
               //fmt.Printf("Node %d -> Node %d: ")
               chanSet[node.ID][node.ID-i] <- tmpNd
            }
            /*if nodeSet[node.ID+i].blockchain != nil || len(chanSet[node.ID][node.ID+i]) > 0 {
               continue
            }*/
         }
      }
      
      if node.active { // node is on
         if Switch.Cmp(CompareOff) < 0 && node.ID % 10 != 0 {
            node.active = false
            fmt.Printf("Node %d is turned Off.\n", node.ID)
         } else {
            for i := 0; i < 5; i++ {
               node.AddLink()
            }

            RandomValue, err = rand.Int(rand.Reader, big.NewInt(12))
            if err != nil {
               fmt.Println("Run_2")
               log.Panic(err)
            }
            Percent := *RandomValue
            if node.attacker == false {
               node.UpdateDB()
            }

            delay := 100
            time.Sleep(time.Duration(delay) * time.Millisecond) 

            if Percent.Cmp(big.NewInt(4)) < 0 {
               node.MakeTransaction()
            } else if Percent.Cmp(big.NewInt(8)) < 0 {
               node.MiningBlock()
            } else {
               delay = 0
            }

            Group[node.ID] = node.ID
            node.Alarm()

            time.Sleep(time.Duration(delay) * time.Millisecond) 
         }
      } else { // node is off
         if Switch.Cmp(CompareOn) < 0 {
            node.active = true
            fmt.Printf("Node %d is turned On.\n", node.ID)

            if node.blockchain == nil {
               //fmt.Printf("Node %d start to make a Blockchain.\n", node.ID)

               sigSet[node.ID][(node.ID/10+1)*10] <- true
               for len(chanSet[(node.ID/10+1)*10][node.ID]) == 0 {
                  time.Sleep(10*time.Millisecond)
               }

               from := <-chanSet[(node.ID/10+1)*10][node.ID]
               node.blockchain = CopyBlockchain(node.ID, from.ID, from.blockchain.Length)
               //fmt.Printf("Node %d has a Blockchain.\n", node.ID)
               node.utxoSet = UTXOSet{node.blockchain}
               //fmt.Printf("Node %d create a UTXOSet.\n", node.ID)
               node.utxoSet.Reindex()
               //fmt.Printf("Node %d has a UTXOSet.\n", node.ID)
               node.UpdateMempool(from.mempool)

               path := fmt.Sprintf(dbFile, from.ID)
               os.Remove(path)
               path = fmt.Sprintf(mpFile, from.ID)
               os.Remove(path)
            }//get data from center node
         }
      }
      time.Sleep(Time_Delay * time.Millisecond) //Time_Delay Second

      for Share_Now {
         if node.attacker == false {
            node.UpdateDB()
            node.Alarm()
         }
      }
      
      //End_Time = time.Now().UnixNano()
      //fmt.Printf("%d: %d %d\n", End_Time-Start_Time, TransactionCnt, BlockCnt)
      //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
      //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))
   }
} 

func (node *Node) AddLink() {
   if node.out < Num_LinkOut {
      RandomValue, err := rand.Int(rand.Reader, big.NewInt(int64(Num_Basic + Num_Attacker + 1)))
      if err != nil {
         fmt.Println("AddLink")
         log.Panic(err)
      }

      willLink := int(RandomValue.Int64())
      if node.attacker {
         willLink = targetNode
      }

      canLink := true
      for _, val := range node.link {
         if val == willLink {
            canLink = false
         }
      }

      if canLink && willLink != node.ID && nodeSet[willLink].in < Num_LinkIn {
         nodeSet[willLink].in++
         nodeSet[willLink].link = append(nodeSet[willLink].link, node.ID)
         node.out++
         node.link = append(node.link, willLink)
         fmt.Printf("Node %d is linked with Node %d\n", node.ID, willLink)
      }
   }
}

func (node *Node) GetBalance() int {
   if node.wallet == nil {
      return 0
   }

   address := node.wallet.GetAddress()
   pubKeyHash := Base58Decode([]byte(address))
   pubKeyHash = pubKeyHash[1 : len(pubKeyHash)-4]
   UTXOs := node.utxoSet.FindUTXO(pubKeyHash)
   //download UTXOs

   balance := 0
   for _, out := range UTXOs {
      balance += out.Value
   }

   return balance
}

func (node *Node) MakeTransaction() {
   balance := node.GetBalance()
   
   fmt.Printf("Node %d has %d BTC\n", node.ID, balance)

   if balance == 0 {
      return
   }

   RandomValue, err := rand.Int(rand.Reader, big.NewInt(int64(balance)))
   if err != nil {
      fmt.Println("MakeTransaction_1")
      log.Panic(err)
   }
   usingAmount := RandomValue.Int64() + 1

   /*if node.ID % 10 == 0 && int64(balance)-usingAmount < remainOff[node.ID/10]*subsidy/2 {
      return
   }*/

   RandomValue, err = rand.Int(rand.Reader, big.NewInt(int64(Num_Basic + Num_Attacker)))
      if err != nil {
      fmt.Println("MakeTransaction_2")
      log.Panic(err)
   }
   toNode := RandomValue.Int64() + 1
   if node.ID == int(toNode) || nodeSet[toNode].wallet == nil {
        return
   }

   //decide Amout and To

   node.Send(int(toNode), int(usingAmount))
   /*toAddress := nodeSet[toNode].wallet.GetAddress()
   
   fmt.Printf("Node %d -> Node %d: %d BTC\n", node.ID, toNode, usingAmount)

   newTx := node.NewUTXOTransaction(node.wallet, string(toAddress), int(usingAmount))

   db, err := bolt.Open(node.mempool, 0600, nil)
   if err != nil {
      fmt.Println("MakeTransaction_3")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(mempoolBucket))
      bucket.Put([]byte(newTx.Serialize()), []byte(newTx.Serialize()))
      return nil
   })
   if err != nil {
      fmt.Println("MakeTransaction_4")
      log.Panic(err)
   }
   db.Close()*/
}

func (node *Node) Send(toNode int, usingAmount int) {
   balance := node.GetBalance()
   
   fmt.Printf("Node %d has %d BTC\n", node.ID, balance)

   if balance < usingAmount || nodeSet[toNode].wallet == nil {
        return
   }

   toAddress := nodeSet[toNode].wallet.GetAddress()
   
   fmt.Printf("Node %d -> Node %d: %d BTC\n", node.ID, toNode, usingAmount)

   newTx := node.NewUTXOTransaction(node.wallet, string(toAddress), usingAmount)

   db, err := bolt.Open(node.mempool, 0600, nil)
   if err != nil {
      fmt.Println("MakeTransaction_3")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(mempoolBucket))
      bucket.Put([]byte(newTx.Serialize()), []byte(newTx.Serialize()))
      return nil
   })
   if err != nil {
      fmt.Println("MakeTransaction_4")
      log.Panic(err)
   }
   db.Close()

   TransactionCnt++
}

func (node *Node) MiningBlock() {
   //fmt.Printf("Node %d is Mining.\n", node.ID)

   var TxSet []string
   var cnt int

   mp, err := bolt.Open(node.mempool, 0600, nil)
   if err != nil {
      fmt.Println("MiningBlock_1")
      log.Panic(err)
   }

   err = mp.Update(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(mempoolBucket))
      cnt = 0
      err = bucket.ForEach(func(_, value []byte) error {
         TxSet = append(TxSet, string(value))
         cnt++
         if cnt >= MinTx {
            return nil
         }
         return nil
      })
      if err != nil {
         fmt.Println("MiningBlock_2")
         log.Panic(err)
      }

      if cnt >= MinTx {
         for _, key := range TxSet {
            bucket.Delete([]byte(key))
         }
      }

      return nil
   })
   if err != nil {
      fmt.Println("MiningBlock_3")
      log.Panic(err)
   }
   mp.Close()

   if cnt < MinTx {
        //fmt.Printf("Node %d: Transactions are not enough For Mining\n", node.ID)
      return
   } else {
      var Txs []Transaction
      for _, tx := range TxSet {
         Txs = append(Txs, DeserializeTransaction([]byte(tx)))
      }
      var TxsAddress []*Transaction
      for _, tx := range Txs {
         TxsAddress = append(TxsAddress, &tx)
      }
      TxsAddress = append(TxsAddress, NewCoinBaseTX(string(node.wallet.GetAddress()), genesisCoinbaseData, false))
      node.AddBlock(TxsAddress)
      BlockCnt++
      fmt.Printf("Node %d's Mining is Successed.\n", node.ID)
      
      return
   }
}

func (node *Node) UpdateDB() {
   cnt := 0
   for len(node.forUpdate) != 0 {
      UpdateID := node.forUpdate[0]
      //fmt.Println("update st")

      End_Time = time.Now().UnixNano()
      //fmt.Printf("%f: %f %f\n", float64(End_Time-Start_Time), float64(TransactionCnt), float64(BlockCnt))
      //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
      //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))

      tmpNd := <-chanSet[UpdateID][node.ID]

      if len(node.forUpdate) == 1 {
         node.forUpdate = node.forUpdate[:0]
      } else {
         node.forUpdate = node.forUpdate[1:]
      }
      if node.blockchain.Length < tmpNd.blockchain.Length {
         Group[node.ID] = Group[tmpNd.ID/(Num_Basic + Num_Attacker + 1) - 1]

         node.blockchain = CopyBlockchain(node.ID, tmpNd.ID, tmpNd.blockchain.Length)
         node.utxoSet = UTXOSet{node.blockchain}
         node.utxoSet.Reindex()
         node.UpdateMempool(tmpNd.mempool)
         fmt.Printf("Node %d is Updated by Node %d\n", node.ID, UpdateID)

         path := fmt.Sprintf(dbFile, tmpNd.ID)
         os.Remove(path)
         path = fmt.Sprintf(mpFile, tmpNd.ID)
         os.Remove(path)
         node.Alarm()
      }
      cnt++
   }  
}

func (node *Node) Alarm() {
   for _, val := range node.link {
      //fmt.Println(val)
      //fmt.Printf("Node %d alarms Node %d: %d\n", node.ID, val, len(chanSet[node.ID][val]))
         
      if val != node.ID && nodeSet[val].active && nodeSet[val].blockchain != nil && len(chanSet[node.ID][val]) == 0{
         nodeSet[val].forUpdate = append(nodeSet[val].forUpdate, node.ID)
         tmpIndex := ((Num_Basic + Num_Attacker + 1)*(node.ID + 1) + val)
         mp := fmt.Sprintf(mpFile, tmpIndex)
         tmpNd := Node{tmpIndex, nil, UTXOSet{nil}, nil, mp, nil, []int{}, false, false, false, 0, 0}
         tmpNd.blockchain = CopyBlockchain(tmpIndex, node.ID, node.blockchain.Length)
         tmpNd.utxoSet = UTXOSet{node.blockchain}
         tmpNd.utxoSet.Reindex()
         tmpNd.UpdateMempool(node.mempool)
         chanSet[node.ID][val] <- tmpNd
      }
   }
}

//blockchain

type Blockchain struct {
   LastHash string
   DBFile string
   Length int
}

type BlockchainIterator struct {
   currentHash string
   DBFile string
}

func CreateBlockchain(address string, nodeID int) *Blockchain {
   DBFile := fmt.Sprintf(dbFile, nodeID)
   /*if dbExists(dbFile) {
      fmt.Println("Blockchain already exists.")
      os.Exit(1)
   }*/

   var tip string

   cbtx := NewCoinBaseTX(address, genesisCoinbaseData, true)
   genesis := NewGenesisBlock(cbtx)

   db, err := bolt.Open(DBFile, 0600, nil)
   if err != nil {
      fmt.Println("CreateBlockchain_1")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      _, err = tx.CreateBucketIfNotExists([]byte(blockBucket))
      if err != nil {
         fmt.Println("CreateBlockchain_2")
         log.Panic(err)
      }

      err := tx.DeleteBucket([]byte(blockBucket))
      if err != nil {
         fmt.Println("CreateBlockchain_3")
         log.Panic(err)
      }

      b, err := tx.CreateBucket([]byte(blockBucket))
      if err != nil {
         fmt.Println("CreateBlockchain_4")
         log.Panic(err)
      }

      err = b.Put(genesis.Hash, genesis.Serialize())
      if err != nil {
         fmt.Println("CreateBlockchain_5")
         log.Panic(err)
      }

      err = b.Put([]byte("l"), genesis.Hash)
      if err != nil {
         fmt.Println("CreateBlockchain_6")
         log.Panic(err)
      }
      tip = string(genesis.Hash)

      return nil
   })
   if err != nil {
      fmt.Println("CreateBlockchain_7")
      log.Panic(err)
   }

   bc := Blockchain{tip, DBFile, 1}

   db.Close()

   return &bc
}

func CopyBlockchain(nodeID int, from int, Len int) *Blockchain{
   var tip string
   DBFile := fmt.Sprintf(dbFile, nodeID)
   DBFile0 := fmt.Sprintf(dbFile, from)

   db, err := bolt.Open(DBFile, 0600, nil)
   if err != nil {
      fmt.Println("CopyBlockchain_1")
      log.Panic(err)
   }
   db0, err := bolt.Open(DBFile0, 0600, nil)
   if err != nil {
      fmt.Println("CopyBlockchain_2")
      log.Panic(err)
   }

   //defer db0.Close()
   //defer db.Close()

   err = db.Update(func(tx *bolt.Tx) error {
      _, err = tx.CreateBucketIfNotExists([]byte(blockBucket))
      if err != nil {
         fmt.Println("CopyBlockchain_3")
         log.Panic(err)
      }

      err := tx.DeleteBucket([]byte(blockBucket))
      if err != nil {
         fmt.Println("CopyBlockchain_4")
         log.Panic(err)
      }

      b, err := tx.CreateBucket([]byte(blockBucket))
      if err != nil {
         fmt.Println("CopyBlockchain_5")
         log.Panic(err)
      }

      err = db0.View(func(tx0 *bolt.Tx) error {
         b0 := tx0.Bucket([]byte(blockBucket))
         tip = string(b0.Get([]byte("l")))
         c0 := b0.Cursor()

         for k, v := c0.First(); k != nil; k, v = c0.Next() {
            err = b.Put(k, v)
            if err != nil {
               fmt.Println("CopyBlockchain_6")
               log.Panic(err)
            }
         }
         return nil
      })
      return nil
   })
   if err != nil {
      fmt.Println("CopyBlockchain_7")
      log.Panic(err)
   }
   bc := Blockchain{tip, DBFile, Len}

   db0.Close()
   db.Close()

   return &bc
}

func (bc *Blockchain) Iterator() *BlockchainIterator {
   bci := &BlockchainIterator{bc.LastHash, bc.DBFile}
   return bci
}

func (bci *BlockchainIterator) Next() *Block {
   var block *Block
   db, err := bolt.Open(bci.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("Next_1")
      log.Panic(err)
   }

   err = db.View(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(blockBucket))
      raw_block := bucket.Get([]byte(bci.currentHash))
      //fmt.Print("Next: ")
      //fmt.Println([]byte(bci.currentHash))
      block = Deserialize(raw_block)
      return nil
   })

   if err != nil {
      fmt.Println("Next_2")
      log.Panic(err)
   }

   db.Close()

   bci.currentHash = string(block.PrevBlockHash)
   return block
}

func (bc *Blockchain) FindEveryUTXO() map[string]TXOutputs {
   UTXO := make(map[string]TXOutputs)
   spentTXOs := make(map[string][]int)
   bci := bc.Iterator()
   for {
      //fmt.Print("FindEveryUTXO: ")
      //fmt.Println([]byte(bci.currentHash))
      /*if bytes.Compare([]byte(bci.currentHash), nil) == 0 {
         break
      }*/
      block := bci.Next()

      for _, tx := range block.Transactions {
         txID := hex.EncodeToString(tx.ID)

      Outputs:
         for outIdx, out := range tx.Vout {
            // Was the output spent?
            if spentTXOs[txID] != nil {
               for _, spentOutIdx := range spentTXOs[txID] {
                  if spentOutIdx == outIdx {
                     continue Outputs
                  }
               }
            }

            outs := UTXO[txID]
            outs.Outputs = append(outs.Outputs, out)
            UTXO[txID] = outs
         }

         if tx.IsCoinbase() == false {
            for _, in := range tx.Vin {
               inTxID := hex.EncodeToString(in.Txid)
               spentTXOs[inTxID] = append(spentTXOs[inTxID], in.Vout)
            }
         }
      }

      if len(block.PrevBlockHash) == 0 {
         break
      }
   }
   return UTXO
}//for reindexing

//Block

type Block struct {
   Timestamp     int64
   Transactions  []*Transaction
   PrevBlockHash []byte
   Hash          []byte
   Nonce         int
}

func NewGenesisBlock(coinbase *Transaction) *Block {
   return NewBlock([]*Transaction{coinbase}, []byte{})
}

func NewBlock(transactions []*Transaction, prevBlockHash []byte) *Block {
   block := &Block{time.Now().Unix(), transactions, prevBlockHash, []byte{}, 0}
   pow := NewPOW(block)
   nonce, hash := pow.Run()
   //fmt.Println("Mining End")

   block.Hash = hash[:]
   block.Nonce = nonce

   //fmt.Print("Block's Hash: ")
   //fmt.Println(block.PrevBlockHash)

   return block
}

func (node *Node) AddBlock(transactions []*Transaction) {
   var tip string

   for _, tx := range transactions {
      if tx.IsCoinbase() == false && node.blockchain.VerifyTransaction(tx) != true {
         fmt.Println("AddBlock_1")
         log.Panic("Error: Invalid Transaction")
      }
   }

   db, err := bolt.Open(node.blockchain.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("Next_2")
      log.Panic(err)
   }
   err = db.View(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(blockBucket))
      tip = string(bucket.Get([]byte("l")))
      return nil
   })
   if err != nil {
      fmt.Println("Next_3")
      log.Panic(err)
   }
   db.Close()

   newBlock := NewBlock(transactions, []byte(tip))

   db, err = bolt.Open(node.blockchain.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("Next_4")
      log.Panic(err)
   }
   err = db.Update(func(tx *bolt.Tx) error {
      bucket := tx.Bucket([]byte(blockBucket))
      bucket.Put([]byte("l"), newBlock.Hash)
      bucket.Put(newBlock.Hash, newBlock.Serialize())
      return nil
   })
   if err != nil {
      fmt.Println("Next_5")
      log.Panic(err)
   }
   db.Close()

   node.blockchain.Length++
   node.blockchain.LastHash = string(newBlock.Hash)

   node.utxoSet.Update(newBlock)
   fmt.Printf("Node %d is updated Successfully.\n", node.ID)         
}

func (b *Block) HashTransactions() []byte {
   var result [32]byte
   var txHashes [][]byte
   for _, tx := range b.Transactions {
      txHashes = append(txHashes, tx.ID)
   }
   result = sha256.Sum256(bytes.Join(txHashes, []byte{}))
   return result[:]
   /*var transactions [][]byte
   for _, tx := range b.Transactions {
      transactions  = append(transactions, tx.Serialize())
   }
   mTree := NewMerkleTree(transactions)
   return mTree.RootNode.Data*/
}

func (b *Block) Serialize() []byte {
   var result bytes.Buffer

   encoder := gob.NewEncoder(&result)
   err := encoder.Encode(b)

   if err != nil {
      fmt.Println("Serialize")
      log.Panic(err)
   }

   return result.Bytes()
}

func Deserialize(d []byte) *Block {
   var block Block

   decoder := gob.NewDecoder(bytes.NewReader(d))
   err := decoder.Decode(&block)
   
   if err != nil {
      fmt.Println("Deserialize")
      log.Panic(err)
   }

   return &block
}

//base 58
var b58Alphabet = []byte("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

func Base58Encode(input []byte) []byte {
   var result []byte

   x := big.NewInt(0).SetBytes(input)

   base := big.NewInt(int64(len(b58Alphabet)))
   zero := big.NewInt(0)
   mod := &big.Int{}

   for x.Cmp(zero) != 0 {
      x.DivMod(x, base, mod)
      result = append(result, b58Alphabet[mod.Int64()])
   }

   if input[0] == 0x00 {
      result = append(result, b58Alphabet[0])
   }

   ReverseBytes(result)

   return result
}

func ReverseBytes(data []byte) {
   for i, j := 0, len(data)-1; i < j; i, j = i+1, j-1 {
      data[i], data[j] = data[j], data[i]
   }
}

func Base58Decode(input []byte) []byte {
   result := big.NewInt(0)

   for _, b := range input {
      charIndex := bytes.IndexByte(b58Alphabet, b)
      result.Mul(result, big.NewInt(58))
      result.Add(result, big.NewInt(int64(charIndex)))
   }

   decoded := result.Bytes()

   if input[0] == b58Alphabet[0] {
      decoded = append([]byte{0x00}, decoded...)
   }

   return decoded
}

//POW

type POW struct {
   block *Block
   target *big.Int
}

func NewPOW(b *Block) *POW {
   target := big.NewInt(1)
   target.Lsh(target, uint(256-targetBits))

   pow := &POW{b, target}
   return pow
}

func (pow *POW) prepareData(nonce int) []byte {
   data := bytes.Join(
      [][]byte {
         pow.block.PrevBlockHash,
         pow.block.HashTransactions(),
         IntToHex(pow.block.Timestamp),
         IntToHex(int64(targetBits)),
         IntToHex(int64(nonce)),
      },
      []byte{},
   )
   return data
}

func IntToHex(n int64) []byte {
    return []byte(strconv.FormatInt(n, 16))
}

const maxNonce = math.MaxInt64

func (pow *POW) Run() (int, []byte) {
   var hashInt big.Int
   var hash [32]byte
   nonce := 0

   //fmt.Printf("Mining the block containing \"%s\"\n", pow.block.Data)

   //fmt.Println("")
   for nonce < maxNonce {
      data := pow.prepareData(nonce)
      hash = sha256.Sum256(data)
   //   fmt.Printf("\r%x", hash)

      hashInt.SetBytes(hash[:])
      if hashInt.Cmp(pow.target) == -1 {
         break;
      } else {
         nonce++
      }
   }
   //fmt.Println("end")

   return nonce, hash[:]
}

func (pow *POW) Validate() bool {
    var hashInt big.Int

    data := pow.prepareData(pow.block.Nonce)
    hash := sha256.Sum256(data)
    hashInt.SetBytes(hash[:])

    isValid := hashInt.Cmp(pow.target) == -1
    return isValid
}

//wallet

type Wallet struct {
   PrivateKey ecdsa.PrivateKey
   PublicKey []byte
}

func NewWallet() *Wallet {
   private, public := newKeyPair()
   wallet := Wallet{private, public}
   return &wallet
}

func newKeyPair() (ecdsa.PrivateKey, []byte) {
   curve := elliptic.P256()
   private, _ := ecdsa.GenerateKey(curve, rand.Reader)
   public := append(private.PublicKey.X.Bytes(), private.PublicKey.Y.Bytes()...)
   return *private, public
}

func (w Wallet) GetAddress() []byte {
   pubKeyHash := HashPubKey(w.PublicKey)

   versionedPayload := append([]byte{version}, pubKeyHash...)
   checksum := checksum(versionedPayload)

   fullPayload := append(versionedPayload, checksum...)
   address := Base58Encode(fullPayload)

   return address
}

func HashPubKey(pubKey []byte) []byte {
   publicSHA256 := sha256.Sum256(pubKey)

   RIPEMD160Hasher := ripemd160.New()
   _, err := RIPEMD160Hasher.Write(publicSHA256[:])
   if err != nil {
      fmt.Println("HashPubKey")
      log.Panic(err)
   }
   publicRIPEMD160 := RIPEMD160Hasher.Sum(nil)

   return publicRIPEMD160
}

const addressChecksumLen = 4

func checksum(payload []byte) []byte {
   firstSHA := sha256.Sum256(payload)
   secondSHA := sha256.Sum256(firstSHA[:])

   return secondSHA[:addressChecksumLen]
}

//transaction

type Transaction struct {
   ID []byte
   Vin []TXInput
   Vout []TXOutput
}

type TXOutput struct {
   Value int
   PubKeyHash []byte
   //used bool
}

type TXInput struct {
   Txid []byte
   Vout int
   Signature []byte
   PubKey []byte
}

type TXOutputs struct {
   Outputs []TXOutput
}

func NewTXOutput(value int, address string) *TXOutput {
   txo := &TXOutput{value, nil/*, false*/}
   txo.Lock([]byte(address))

   return txo
}

func (in *TXInput) UsesKey(pubKeyHash []byte) bool {
   lockingHash := HashPubKey(in.PubKey)
   return bytes.Compare(lockingHash, pubKeyHash) == 0
}

func (out *TXOutput) Lock(address []byte) {
   pubKeyHash := Base58Decode(address)
   pubKeyHash = pubKeyHash[1:len(pubKeyHash)-4]
   out.PubKeyHash = pubKeyHash
}

func (out *TXOutput) IsLockedWithKey(pubKeyHash []byte) bool {
   return bytes.Compare(out.PubKeyHash, pubKeyHash) == 0
}

func (outs TXOutputs) Serialize() []byte {
   var buff bytes.Buffer

   enc := gob.NewEncoder(&buff)
   err := enc.Encode(outs)
   if err != nil {
      fmt.Println("Serialize")
      log.Panic(err)
   }

   return buff.Bytes()
}

func DeserializeOutputs(data []byte) TXOutputs {
   var outputs TXOutputs

   dec := gob.NewDecoder(bytes.NewReader(data))
   err := dec.Decode(&outputs)
   if err != nil {
      fmt.Println("DeserializeOutputs")
      log.Panic(err)
   }

   return outputs
}

func (tx Transaction) IsCoinbase() bool {
   return len(tx.Vin) == 1 && len(tx.Vin[0].Txid) == 0 && tx.Vin[0].Vout == -1
}

func (tx *Transaction) Hash() []byte {
   var hash [32]byte

   txCopy := *tx
   txCopy.ID = []byte{}

   hash = sha256.Sum256(txCopy.Serialize())

   return hash[:]
}

func (tx *Transaction) Serialize() []byte {
   var result bytes.Buffer

   encoder := gob.NewEncoder(&result)
   err := encoder.Encode(tx)

   if err != nil {
      fmt.Println("Serialize")
      log.Panic(err)
   }

   return result.Bytes()
}

func DeserializeTransaction(data []byte) Transaction {
   var transaction Transaction

   decoder := gob.NewDecoder(bytes.NewReader(data))
   err := decoder.Decode(&transaction)
   if err != nil {
      fmt.Println("DeserializeTransaction")
      log.Panic(err)
   }

   return transaction
}

func (tx *Transaction) Sign(privateKey ecdsa.PrivateKey, prevTxs map[string]Transaction) {
   if(tx.IsCoinbase()) {
      return
   }

   txCopy := tx.TrimmedCopy()

   for inID, vin := range txCopy.Vin {      
      prevTx := prevTxs[hex.EncodeToString(vin.Txid)]
      txCopy.Vin[inID].Signature = nil
      //fmt.Printf("Processing : %s -> %d\n", hex.EncodeToString(vin.Txid), len(prevTx.Vout))
      
      if vin.Vout >= len(prevTx.Vout) {
         continue
      }

      txCopy.Vin[inID].PubKey = prevTx.Vout[vin.Vout].PubKeyHash
      
      txCopy.ID = txCopy.Hash()
      
      r, s, err := ecdsa.Sign(rand.Reader, &privateKey, txCopy.ID)
      signature := append(r.Bytes(), s.Bytes()...)
      
      tx.Vin[inID].Signature = signature
      txCopy.Vin[inID].PubKey = nil
      if err != nil {
         fmt.Println("Sign")
         log.Panic(err)
      }
   }
}

func (tx *Transaction) TrimmedCopy() Transaction {
   var inputs []TXInput
   var outputs []TXOutput

   for _, vin := range tx.Vin {
      inputs = append(inputs, TXInput{vin.Txid, vin.Vout, nil, nil})
   }

   for _, vout := range tx.Vout {
      outputs = append(outputs, TXOutput{vout.Value, vout.PubKeyHash/*, vout.used*/})
   }

   txCopy := Transaction{tx.ID, inputs, outputs}
   return txCopy
}

func (tx *Transaction) Verify(prevTXs map[string]Transaction) bool {
    txCopy := tx.TrimmedCopy()
    curve := elliptic.P256()

    for inID, vin := range tx.Vin {
        prevTx := prevTXs[hex.EncodeToString(vin.Txid)]
        txCopy.Vin[inID].Signature = nil

         if vin.Vout >= len(prevTx.Vout) {
            continue
         }

        txCopy.Vin[inID].PubKey = prevTx.Vout[vin.Vout].PubKeyHash
        txCopy.ID = txCopy.Hash()
        txCopy.Vin[inID].PubKey = nil

        r := big.Int{}
        s := big.Int{}
        sigLen := len(vin.Signature)
        r.SetBytes(vin.Signature[:(sigLen / 2)])
        s.SetBytes(vin.Signature[(sigLen / 2):])

        x := big.Int{}
        y := big.Int{}
        keyLen := len(vin.PubKey)
        x.SetBytes(vin.PubKey[:(keyLen / 2)])
        y.SetBytes(vin.PubKey[(keyLen / 2):])

        rawPubKey := ecdsa.PublicKey{curve, &x, &y}
        if ecdsa.Verify(&rawPubKey, txCopy.ID, &r, &s) == false {
            return false
        }
    }
    return true
}

func (bc *Blockchain) FindTransaction(ID []byte) (Transaction, error) {
   bci := bc.Iterator()
   for {
      //fmt.Print("FindTransaction before: ")
      //fmt.Println([]byte(bci.currentHash))
      
      /*if bytes.Compare([]byte(bci.currentHash), []byte("")) == 0 {
         break
      }*/ 
      // ???

      block := bci.Next()

      //fmt.Print("FindTransaction after: ")
      //fmt.Println([]byte(bci.currentHash))

      for _, tx := range block.Transactions {
         if bytes.Compare(tx.ID, ID) == 0 {
            return *tx, nil
         }

         /*HashToInt, _ := strconv.Atoi(string(block.PrevBlockHash))
         if HashToInt == 0 {
            break
         }*/
         //???
      }

      if len(block.PrevBlockHash) == 0 {
         break
      }
      //???
   }
   return Transaction{}, errors.New("Transaction is not Found")
}

func (bc *Blockchain) SignTransaction(tx *Transaction, privKey ecdsa.PrivateKey) {
   prevTXs := make(map[string]Transaction)

   for _, vin := range tx.Vin {
      prevTX, err := bc.FindTransaction(vin.Txid)
      prevTXs[hex.EncodeToString(prevTX.ID)] = prevTX
      if err != nil {
         fmt.Println("SignTransaction")
         log.Panic(err)
      }
   }
   tx.Sign(privKey, prevTXs)
}

func (bc *Blockchain) VerifyTransaction(tx *Transaction) bool {
   prevTXs := make(map[string]Transaction)

   for _, vin := range tx.Vin {
      prevTX, err := bc.FindTransaction(vin.Txid)
      prevTXs[hex.EncodeToString(prevTX.ID)] = prevTX
      if err != nil {
         fmt.Println("VerifyTransaction")
         log.Panic(err)
      }
    }

    return tx.Verify(prevTXs)
}

/*func (node *Node) RemoveOutputs(transactions []*Transaction) {
   db, err := bolt.Open(node.blockchain.DBFile, 0600, nil)
    if err != nil {
       log.Panic(err)
    }
    err = db.Update(func(tx *bolt.Tx) error {
        bucket := tx.Bucket([]byte(blockBucket))
        err = bucket.ForEach(func(key, value []byte) error {
         if string(key) != "l" {
            block := Deserialize(value)
            for idx, Tx := range block.Transactions {
               for _, usedTx := range transactions {
                  if string(usedTx.Serialize()) == string(Tx.Serialize()) {
                     block.Transactions[idx] = usedTx
                  }
               }
            }
            bucket.Delete([]byte(key))
            bucket.Put([]byte(key), block.Serialize())   
           }
         return nil
        })
        if err != nil {
         log.Panic(err)
      }
        return nil
    })
    if err != nil {
       log.Panic(err)
    }
    db.Close()
}*/

const subsidy = 20

const genesisCoinbaseData = "The Times 03/Jan/2009 Chancellor on brink of second bailout for banks"

func NewCoinBaseTX(to, data string, genesis bool) *Transaction{
   txin := TXInput{[]byte{}, -1, nil, []byte(data)}
   sub := subsidy
   if genesis {
      sub += (subsidy*(Num_Basic+Num_Attacker)/10+ (subsidy/2)*(Num_Basic+Num_Attacker)*9/10)
   }
   txout := NewTXOutput(sub, to)
   tx := Transaction{nil, []TXInput{txin}, []TXOutput{*txout}}
   tx.ID = tx.Hash()

   return &tx
}

//UTXO

type UTXOSet struct {
   blockchain *Blockchain
}

func (u UTXOSet) FindUTXO(pubKeyHash []byte) []TXOutput {
   var UTXOs []TXOutput
   db, err := bolt.Open(u.blockchain.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("FindUTXO_1")
      log.Panic(err)
   }

   err = db.View(func(tx *bolt.Tx) error {
      b := tx.Bucket([]byte(utxoBucket))
      c := b.Cursor()

      for k, v := c.First(); k != nil; k, v = c.Next() {
         outs := DeserializeOutputs(v)

         for _, out := range outs.Outputs {
            if /*out.used == false && */out.IsLockedWithKey(pubKeyHash) {
               UTXOs = append(UTXOs, out)
            }
         }
      }

      return nil
   })
   if err != nil {
      fmt.Println("FindUTXO_2")
      log.Panic(err)
   }

   db.Close()

   return UTXOs
}

func (u UTXOSet) FindSpendableOutputs(pubkeyHash []byte, amount int) (int, map[string][]int) {
   unspentOutputs := make(map[string][]int)
   accumulated := 0
   DBFile := u.blockchain.DBFile

   db, err := bolt.Open(DBFile, 0600, nil)
   if err != nil {
      fmt.Println("FindSpendableOutputs_1")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      b := tx.Bucket([]byte(utxoBucket))
      c := b.Cursor()

      for k, v := c.First(); k != nil; k, v = c.Next() {
         txID := hex.EncodeToString(k)
         outs := DeserializeOutputs(v)
         //var reOuts TXOutputs

         //fmt.Printf("Prepare: %s -> %d\n", txID, len(outs.Outputs))

         for outIdx, out := range outs.Outputs {
            if /*out.used == false &&*/ out.IsLockedWithKey(pubkeyHash) && accumulated < amount {
               /*out.used = true*/
               accumulated += out.Value
               unspentOutputs[txID] = append(unspentOutputs[txID], outIdx)
            } /*else {
               reOuts.Outputs = append(reOuts.Outputs, out)
            }*/
         }

         //b.Delete(k)
         //b.Put(k, reOuts.Serialize())
      }

      return nil
   })
   if err != nil {
      fmt.Println("FindSpendableOutputs_2")
      log.Panic(err)
   }

   db.Close()

   /*after := 0
   db, _ = bolt.Open(DBFile, 0600, nil)

   err = db.View(func(tx *bolt.Tx) error {
      b := tx.Bucket([]byte(utxoBucket))
      c := b.Cursor()

      for k, v := c.First(); k != nil; k, v = c.Next() {
         outs := DeserializeOutputs(v)
         //var reOuts TXOutputs

         for _, out := range outs.Outputs {
            if out.used == false && out.IsLockedWithKey(pubkeyHash){
               out.used = true
               //unspentOutputs[txID] = append(unspentOutputs[txID], outIdx)
            } 
         }
      }

      return nil
   })
   if err != nil {
      log.Panic(err)
   }

   fmt.Printf("%d BTC -> %d BTC\n", accumulated, after)

   db.Close()*/

   return accumulated, unspentOutputs
}

func (u UTXOSet) Reindex() {
   db, err := bolt.Open(u.blockchain.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("Reindex_1")
      log.Panic(err)
   }
   bucketName := []byte(utxoBucket)

   err = db.Update(func(tx *bolt.Tx) error {
      err := tx.DeleteBucket(bucketName)
      if err != nil && err != bolt.ErrBucketNotFound {
         fmt.Println("Reindex_2")
         log.Panic(err)
      }
      _, err = tx.CreateBucket(bucketName)
      if err != nil {
         fmt.Println("Reindex_3")
         log.Panic(err)
      }

      return nil
   })
   if err != nil {
      fmt.Println("Reindex_4")
      log.Panic(err)
   }

   db.Close()

   UTXO := u.blockchain.FindEveryUTXO()

   db, err = bolt.Open(u.blockchain.DBFile, 0600, nil)

   if err != nil {
      fmt.Println("Reindex_5")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      b := tx.Bucket(bucketName)

      for txID, outs := range UTXO {
         key, err := hex.DecodeString(txID)
         if err != nil {
            fmt.Println("Reindex_6")
            log.Panic(err)
         }

         err = b.Put(key, outs.Serialize())
         if err != nil {
            fmt.Println("Reindex_7")
            log.Panic(err)
         }
      }

      return nil
   })
   if err != nil {
      fmt.Println("Reindex_8")
      log.Panic(err)
   }

   db.Close()
}

func (u UTXOSet) Update(block *Block) {
   db, err := bolt.Open(u.blockchain.DBFile, 0600, nil)
   if err != nil {
      fmt.Println("Update_1")
      log.Panic(err)
   }

   err = db.Update(func(tx *bolt.Tx) error {
      b := tx.Bucket([]byte(utxoBucket))

      for _, tx := range block.Transactions {
         if tx.IsCoinbase() == false {
            for _, vin := range tx.Vin {
               updatedOuts := TXOutputs{}
               outsBytes := b.Get(vin.Txid)

               //fmt.Println(outsBytes)
               if bytes.Equal(outsBytes, nil) {
                  continue
               }
               //fmt.Println("ok")

               outs := DeserializeOutputs(outsBytes)

               for outIdx, out := range outs.Outputs {
                  if outIdx != vin.Vout {
                     updatedOuts.Outputs = append(updatedOuts.Outputs, out)
                  }
               }

               if len(updatedOuts.Outputs) == 0 {
                  err := b.Delete(vin.Txid)
                  if err != nil {
                     fmt.Println("Update_2")
                     log.Panic(err)
                  }
               } else {
                  err := b.Put(vin.Txid, updatedOuts.Serialize())
                  if err != nil {
                     fmt.Println("Update_3")
                     log.Panic(err)
                  }
               }

            }
         }

         newOutputs := TXOutputs{}
         for _, out := range tx.Vout {
            newOutputs.Outputs = append(newOutputs.Outputs, out)
         }

         err := b.Put(tx.ID, newOutputs.Serialize())
         if err != nil {
            fmt.Println("Update_4")
            log.Panic(err)
         }
      }

      return nil
   })
   if err != nil {
      fmt.Println("Update_5")
      log.Panic(err)
   }
   db.Close()
}

func (node *Node) NewUTXOTransaction(wallet *Wallet, to string, amount int) *Transaction {
   var inputs []TXInput
   var outputs []TXOutput

   pubKeyHash := HashPubKey(wallet.PublicKey)
   acc, validOutputs := node.utxoSet.FindSpendableOutputs(pubKeyHash, amount)

   if acc < amount {
      fmt.Printf("%d < %d\n", acc, amount)
      fmt.Println("NewUTXOTransaction_1")
      log.Panic("ERROR: Not enough funds")
   }

   // Build a list of inputs
   for txid, outs := range validOutputs {
      txID, err := hex.DecodeString(txid)
      if err != nil {
         fmt.Println("NewUTXOTransaction_2")
         log.Panic(err)
      }

      for _, out := range outs {
         input := TXInput{txID, out, nil, wallet.PublicKey}
         inputs = append(inputs, input)
      }
   }

   // Build a list of outputs
   from := fmt.Sprintf("%s", wallet.GetAddress())
   outputs = append(outputs, *NewTXOutput(amount, to))
   if acc > amount {
      outputs = append(outputs, *NewTXOutput(acc-amount, from)) // a change
   }

   tx := Transaction{nil, inputs, outputs}
   tx.ID = tx.Hash()

   node.utxoSet.blockchain.SignTransaction(&tx, wallet.PrivateKey)

   return &tx
}

//MerkleTree

type MerkleTree struct {
   RootNode *MerkleNode
}

type MerkleNode struct {
   Left *MerkleNode
   Right *MerkleNode
   Data []byte
}

func NewMerkleNode(left, right *MerkleNode, data []byte) *MerkleNode {
   mNode := MerkleNode{}

   if left == nil && right == nil {
      hash := sha256.Sum256(data)
      mNode.Data = hash[:]
   } else {
      prevHashes := append(left.Data, right.Data...)
      hash := sha256.Sum256(prevHashes)
      mNode.Data = hash[:]
   }

   mNode.Left = left
   mNode.Right = right
   return &mNode
}

func NewMerkleTree(data [][]byte) *MerkleTree {
   var nodes []MerkleNode

   if len(data)%2 != 0 {
      data = append(data, data[len(data)-1])
   }

   for _, datum := range data {
      node := NewMerkleNode(nil, nil, datum)
      nodes = append(nodes, *node)
   }

   for i := 0; i < len(data)/2; i++ {
      var newLevel []MerkleNode

      for j := 0; j < len(nodes); j += 2 {
         node := NewMerkleNode(&nodes[j], &nodes[j+1], nil)
         newLevel = append(newLevel, *node)
      }

      nodes = newLevel
   }

   mTree := MerkleTree{&nodes[0]}

   return &mTree
}


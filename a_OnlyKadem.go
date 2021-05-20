package main

import (
   "bytes"
   "container/list"
   "crypto/sha1"
   "crypto/sha256"
   "encoding/gob"
   "fmt"
   "github.com/boltdb/bolt"
   "math"
   "math/big"
   "math/bits"
   "sort"
   "strconv"
   "sync"
   "time"
   //"flag"
   "crypto/ecdsa"
   "crypto/elliptic"
   "encoding/hex"
   "errors"
   "log"
   "os"
   //"math/rand"
   "crypto/rand"
   "golang.org/x/crypto/ripemd160"
)

var nodeSet []*Node
var chanSet [][]chan Node

var IDs []ID

func main() {
   var wait sync.WaitGroup
   wait.Add(Num_Basic + Num_Attacker + 1 + 1)
   // Center Node + Basic Node + Attacker Node + System

   //Time_Start := time.Now().UnixNano()

   //node := []*Node{}
   //0: Center Node, 1~Nb: Basic, Nb+1~Na: Attacker

   for from := 0; from <= Num_Basic+Num_Attacker; from++ {
      var tmpSet []chan Node
      for to := 0; to <= Num_Basic+Num_Attacker; to++ {
         tmp := make(chan Node, 1)
         tmpSet = append(tmpSet, tmp)
      }
      chanSet = append(chanSet, tmpSet)
   }
   // set up for channel

   for idx := 0; idx <= Num_Basic+Num_Attacker; idx++ {
      nd := NewNode(idx)
      /*for to := 0; to <= Num_Basic + Num_Attacker ; to ++ {
         if to != idx {
            nd.link = append(nd.link, to)
         }
      }*/
      nodeSet = append(nodeSet, nd)
   }
   // set up for node


   for _, val := range nodeSet {
      IDs = append(IDs, val.NodeID)    
   }

   BlockCnt = 0
   TransactionCnt = 0
   Attack_S = 0
   Attack_F = 0
   // set up for variable

   for _, nd := range nodeSet {
      fmt.Print(nd.Num)
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

   for i := 0; i <= Num_Basic+Num_Attacker; i++ {
      fmt.Println(i, "", nodeSet[i].GetBalance())
   }

   //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
   //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))
}

const version = byte(0x00)

//var Current_Time int64
var Start_Time int64
var End_Time int64

const k_of_bucket = 20
const alpha = 3

const Period_Reboot = 3000
const Time_Delay = 50

var Cnt_Reboot int

const Limit_Reboot = 5

const Num_Basic = 100
const Num_Attacker = 20

const Num_LinkIn = 10
const Num_LinkOut = 10

const dbFile = "blockchain_%d.db"
const mpFile = "mempool_%d.db"
const blockBucket = "b"
const utxoBucket = "u"
const mempoolBucket = "m"

const targetBits = 14

const MinTx = 1

const targetNode = 1

var Attack_S int64
var Attack_F int64

var BlockCnt int64
var TransactionCnt int64

var canUseDB []bool

func System(wait *sync.WaitGroup) {
   defer wait.Done()
   for Cnt_Reboot <= Limit_Reboot {
      fmt.Printf("Reboot: %d\n", Cnt_Reboot)
      time.Sleep(Period_Reboot * time.Millisecond)

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

      Cnt_Reboot++
      for i := 0; i <= Num_Basic+Num_Attacker; i++ {
         nodeSet[i].link = []int{}
         /*if nodeSet[i].blockchain != nil {
            fmt.Printf("! Node %d has %d BTC / %d \n", i, nodeSet[i].GetBalance(), nodeSet[i].blockchain.Length)
         }*/
      }
   }
   fmt.Printf("Last Reboot\n")
}

type Node struct {
   Num          int
   NodeID       ID
   blockchain   *Blockchain
   utxoSet      UTXOSet
   wallet       *Wallet
   routingtable *RoutingTable
   mempool      string
   link         []int
   forUpdate    []int
   center       bool
   attacker     bool
   active       bool
   in           int
   out          int
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
   returnNode := &Node{index, "", nil, UTXOSet{nil}, nil, nil, mp, nil, []int{}, false, false, false, 0, 0}
   returnNode.NodeID = randSHA1ID()
   m := NewMetrics()
   returnNode.routingtable = NewRoutingTable(k_of_bucket, ConvertPeerID(returnNode.NodeID), time.Hour, m)
   for i, _ := range IDs {
      returnNode.routingtable.Update(IDs[i])
   }
   fmt.Println(returnNode.routingtable.Size())
   CreateMempool(returnNode.mempool)
   if index%10 == 0 {
      returnNode.center = true
      returnNode.wallet = NewWallet()
      returnNode.active = true
      address := string(returnNode.wallet.GetAddress())
      returnNode.blockchain = CreateBlockchain(address, returnNode.Num)
      returnNode.utxoSet = UTXOSet{returnNode.blockchain}
      returnNode.utxoSet.Reindex()
   }
   if index > Num_Basic {
      returnNode.attacker = true
   }
   return returnNode
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

      if node.Num%10 == 0 {
         for i := 1; i < 10 && node.Num+i <= Num_Basic+Num_Attacker; i++ {
            if nodeSet[node.Num+i].blockchain != nil || len(chanSet[node.Num][node.Num+i]) > 0 {
               continue
            }
            idx := ((Num_Basic+Num_Attacker+1)*(node.Num+1) + node.Num + i)
            mp := fmt.Sprintf(mpFile, idx)
            tmpNd := Node{idx, "", nil, UTXOSet{nil}, nil, nil, mp, nil, []int{}, false, false, false, 0, 0}
            tmpNd.blockchain = CopyBlockchain(idx, node.Num, node.blockchain.Length)
            tmpNd.utxoSet = UTXOSet{node.blockchain}
            tmpNd.utxoSet.Reindex()
            tmpNd.UpdateMempool(node.mempool)
            chanSet[node.Num][node.Num+i] <- tmpNd
         }
      }

      if node.active { // node is on
         if Switch.Cmp(CompareOff) < 0 && node.Num%10 != 0 {
            node.active = false
            fmt.Printf("Node %d is turned Off.\n", node.Num)
         } else {
            node.AddLinkbyKadem()
            node.AddLinkbyKadem()
            node.AddLinkbyKadem()
            RandomValue, err = rand.Int(rand.Reader, big.NewInt(12))
            if err != nil {
               fmt.Println("Run_2")
               log.Panic(err)
            }
            Percent := *RandomValue
            if node.attacker == false {
               node.UpdateDB()
            }
            if Percent.Cmp(big.NewInt(6)) < 0 {
               node.MakeTransaction()
            } else {
               node.MiningBlock()
            }
            node.Alarm()
         }
      } else { // node is off
         if Switch.Cmp(CompareOn) < 0 {
            node.active = true
            fmt.Printf("Node %d is turned On.\n", node.Num)

            if node.wallet == nil {
               node.wallet = NewWallet()
            }

            if node.blockchain == nil {
               fmt.Printf("Node %d start to make a Blockchain.\n", node.Num)
               for len(chanSet[(node.Num/10)*10][node.Num]) == 0 {
                  time.Sleep(5 * time.Millisecond)
                  //fmt.Println("I'm stuck")
               }

               from := <-chanSet[(node.Num/10)*10][node.Num]
               node.blockchain = CopyBlockchain(node.Num, from.Num, from.blockchain.Length)
               //fmt.Printf("Node %d has a Blockchain.\n", node.Num)
               node.utxoSet = UTXOSet{node.blockchain}
               //fmt.Printf("Node %d create a UTXOSet.\n", node.Num)
               node.utxoSet.Reindex()
               //fmt.Printf("Node %d has a UTXOSet.\n", node.Num)
               node.UpdateMempool(from.mempool)

               path := fmt.Sprintf(dbFile, from.Num)
               os.Remove(path)
               path = fmt.Sprintf(mpFile, from.Num)
               os.Remove(path)
            } //get data from center node
         }
      }
      time.Sleep(Time_Delay * time.Millisecond) //Time_Delay Second

      //End_Time = time.Now().UnixNano()
      //fmt.Printf("%d: %d %d\n", End_Time-Start_Time, TransactionCnt, BlockCnt)
      //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
      //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))
   }
}

func (node *Node) AddLinkbyKadem() {
   if node.out < Num_LinkOut {
      RandomValue, err := rand.Int(rand.Reader, big.NewInt(int64(Num_Basic + Num_Attacker + 1)))
      if err != nil {
         fmt.Println("AddLinkbyKadem")
         log.Panic(err)
      }

      willLink := int(RandomValue.Int64())
      IDofwillLink := nodeSet[willLink].NodeID
      if node.attacker {
         willLink = targetNode
         IDofwillLink := nodeSet[willLink].NodeID
         fmt.Printf("%d(%x)->%d(%x), Linking by Attacker Setup...\n", node.Num, node.NodeID, willLink, IDofwillLink)
      } else {
         RandomP, _ := rand.Int(rand.Reader, big.NewInt(int64(10)))

         if RandomP.Cmp(big.NewInt(3)) > 0 {
            fmt.Printf("%d(%x)->%d(%x), Linking by RoutingTable Setup...\n", node.Num, node.NodeID, willLink, IDofwillLink)
            Nears := node.routingtable.NearestPeers(ConvertPeerID(node.NodeID), alpha)
            RandomNum, _ := rand.Int(rand.Reader, big.NewInt(int64(alpha)))
            time.Sleep(time.Second)
            IDofwillLink = Nears[int(RandomNum.Int64())]
            for _, val := range nodeSet {
               if IDofwillLink == val.NodeID {
                  willLink = val.Num
                  break
               }
            }
         } else {
            fmt.Printf("%d(%x)->%d(%x), Linking by Random Setup..\n", node.Num, node.NodeID, willLink, IDofwillLink)
         }
      }

      canLink := true
      for _, val := range node.link {
         if val == willLink {
            canLink = false
            fmt.Printf("%d->%d, Already Linked ", node.Num, willLink)
         }
      }

      if canLink && willLink != node.Num && nodeSet[willLink].in < Num_LinkIn {
         fmt.Printf("Linking...")
         node.routingtable.Find(IDofwillLink)
         node.routingtable.Update(nodeSet[willLink].NodeID)
         nodeSet[willLink].routingtable.Update(node.NodeID)
         nodeSet[willLink].in++
         nodeSet[willLink].link = append(nodeSet[willLink].link, node.Num)
         node.out++
         node.link = append(node.link, willLink)
         fmt.Printf("%d->%d, Successfully linked\n", node.Num, willLink)
         return
      }
      fmt.Printf("%d->%d, Linking Failed\n", node.Num, willLink)
   }
}

func (node *Node) AddLink() {
   if node.out < Num_LinkOut {
      RandomValue, err := rand.Int(rand.Reader, big.NewInt(int64(Num_Basic+Num_Attacker+1)))
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

      if canLink && willLink != node.Num && nodeSet[willLink].in < Num_LinkIn {
         nodeSet[willLink].in++
         nodeSet[willLink].link = append(nodeSet[willLink].link, node.Num)
         node.out++
         node.link = append(node.link, willLink)
         fmt.Printf("Node %d is linked with Node %d\n", node.Num, willLink)
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

   fmt.Printf("Node %d has %d BTC\n", node.Num, balance)

   if balance == 0 {
      return
   }
   //decide Amout and To

   RandomValue, err := rand.Int(rand.Reader, big.NewInt(int64(balance)))
   if err != nil {
      fmt.Println("MakeTransaction_1")
      log.Panic(err)
   }
   usingAmount := RandomValue.Int64() + 1

   RandomValue, err = rand.Int(rand.Reader, big.NewInt(int64(Num_Basic+Num_Attacker)))
   if err != nil {
      fmt.Println("MakeTransaction_2")
      log.Panic(err)
   }
   toNode := RandomValue.Int64() + 1
   if node.Num == int(toNode) || nodeSet[toNode].wallet == nil {
      return
   }
   toAddress := nodeSet[toNode].wallet.GetAddress()

   fmt.Printf("Node %d -> Node %d: %d BTC\n", node.Num, toNode, usingAmount)

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
   db.Close()

   TransactionCnt++
}

func (node *Node) MiningBlock() {
   //fmt.Printf("Node %d is Mining.\n", node.Num)

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
      //fmt.Printf("Node %d: Transactions are not enough For Mining\n", node.Num)
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
      TxsAddress = append(TxsAddress, NewCoinBaseTX(string(node.wallet.GetAddress()), genesisCoinbaseData))
      node.AddBlock(TxsAddress)
      BlockCnt++
      fmt.Printf("Node %d's Mining is Successed.\n", node.Num)

      return
   }
}

func (node *Node) UpdateDB() {
   cnt := 0
   for len(node.forUpdate) != 0 && cnt < 5 {
      UpdateID := node.forUpdate[0]
      //fmt.Println("update st")

      End_Time = time.Now().UnixNano()
      //fmt.Printf("%f: %f %f\n", float64(End_Time-Start_Time), float64(TransactionCnt), float64(BlockCnt))
      //fmt.Printf("%f Transactions per seconds\n", float64(TransactionCnt)/float64(End_Time-Start_Time))
      //fmt.Printf("%f Blocks per seconds\n", float64(BlockCnt)/float64(End_Time-Start_Time))

      tmpNd := <-chanSet[UpdateID][node.Num]

      if len(node.forUpdate) == 1 {
         node.forUpdate = node.forUpdate[:0]
      } else {
         node.forUpdate = node.forUpdate[1:]
      }
      if node.blockchain.Length < tmpNd.blockchain.Length {
         node.blockchain = CopyBlockchain(node.Num, tmpNd.Num, tmpNd.blockchain.Length)
         node.utxoSet = UTXOSet{node.blockchain}
         node.utxoSet.Reindex()
         node.UpdateMempool(tmpNd.mempool)
         fmt.Printf("Node %d is Updated by Node %d\n", node.Num, UpdateID)

         path := fmt.Sprintf(dbFile, tmpNd.Num)
         os.Remove(path)
         path = fmt.Sprintf(mpFile, tmpNd.Num)
         os.Remove(path)
         node.Alarm()
      }
      cnt++
   }
}

func (node *Node) Alarm() {
   for _, val := range node.link {
      //fmt.Println(val)
      if val != node.Num && len(chanSet[node.Num][val]) == 0 {
         nodeSet[val].forUpdate = append(nodeSet[val].forUpdate, node.Num)
         tmpIndex := ((Num_Basic+Num_Attacker+1)*(node.Num+1) + val)
         mp := fmt.Sprintf(mpFile, tmpIndex)
         tmpNd := Node{tmpIndex, "", nil, UTXOSet{nil}, nil, nil, mp, nil, []int{}, false, false, false, 0, 0}
         tmpNd.blockchain = CopyBlockchain(tmpIndex, node.Num, node.blockchain.Length)
         tmpNd.utxoSet = UTXOSet{node.blockchain}
         tmpNd.utxoSet.Reindex()
         tmpNd.UpdateMempool(node.mempool)
         chanSet[node.Num][val] <- tmpNd
      }
   }
}

//blockchain

type Blockchain struct {
   LastHash string
   DBFile   string
   Length   int
}

type BlockchainIterator struct {
   currentHash string
   DBFile      string
}

func CreateBlockchain(address string, nodeID int) *Blockchain {
   DBFile := fmt.Sprintf(dbFile, nodeID)
   /*if dbExists(dbFile) {
      fmt.Println("Blockchain already exists.")
      os.Exit(1)
   }*/

   var tip string

   cbtx := NewCoinBaseTX(address, genesisCoinbaseData)
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

func CopyBlockchain(nodeID int, from int, Len int) *Blockchain {
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
         txID := hex.EncodeToString(tx.TransacID)

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
} //for reindexing

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
   fmt.Printf("Node %d is updated Successfully.\n", node.Num)
}

func (b *Block) HashTransactions() []byte {
   var result [32]byte
   var txHashes [][]byte
   for _, tx := range b.Transactions {
      txHashes = append(txHashes, tx.TransacID)
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
   block  *Block
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
      [][]byte{
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
         break
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
   PublicKey  []byte
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
   TransacID []byte
   Vin       []TXInput
   Vout      []TXOutput
}

type TXOutput struct {
   Value      int
   PubKeyHash []byte
   //used bool
}

type TXInput struct {
   Txid      []byte
   Vout      int
   Signature []byte
   PubKey    []byte
}

type TXOutputs struct {
   Outputs []TXOutput
}

func NewTXOutput(value int, address string) *TXOutput {
   txo := &TXOutput{value, nil /*, false*/}
   txo.Lock([]byte(address))

   return txo
}

func (in *TXInput) UsesKey(pubKeyHash []byte) bool {
   lockingHash := HashPubKey(in.PubKey)
   return bytes.Compare(lockingHash, pubKeyHash) == 0
}

func (out *TXOutput) Lock(address []byte) {
   pubKeyHash := Base58Decode(address)
   pubKeyHash = pubKeyHash[1 : len(pubKeyHash)-4]
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
   txCopy.TransacID = []byte{}

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
   if tx.IsCoinbase() {
      return
   }

   txCopy := tx.TrimmedCopy()

   for inID, vin := range txCopy.Vin {
      prevTx := prevTxs[hex.EncodeToString(vin.Txid)]
      txCopy.Vin[inID].Signature = nil
      txCopy.Vin[inID].PubKey = prevTx.Vout[vin.Vout].PubKeyHash
      txCopy.TransacID = txCopy.Hash()
      txCopy.Vin[inID].PubKey = nil

      r, s, err := ecdsa.Sign(rand.Reader, &privateKey, txCopy.TransacID)
      signature := append(r.Bytes(), s.Bytes()...)
      tx.Vin[inID].Signature = signature
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
      outputs = append(outputs, TXOutput{vout.Value, vout.PubKeyHash /*, vout.used*/})
   }

   txCopy := Transaction{tx.TransacID, inputs, outputs}
   return txCopy
}

func (tx *Transaction) Verify(prevTXs map[string]Transaction) bool {
   txCopy := tx.TrimmedCopy()
   curve := elliptic.P256()

   for inID, vin := range tx.Vin {
      prevTx := prevTXs[hex.EncodeToString(vin.Txid)]
      txCopy.Vin[inID].Signature = nil
      txCopy.Vin[inID].PubKey = prevTx.Vout[vin.Vout].PubKeyHash
      txCopy.TransacID = txCopy.Hash()
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
      if ecdsa.Verify(&rawPubKey, txCopy.TransacID, &r, &s) == false {
         return false
      }
   }
   return true
}

func (bc *Blockchain) FindTransaction(tID []byte) (Transaction, error) {
   bci := bc.Iterator()
   for {
      //fmt.Print("FindTransaction before: ")
      //fmt.Println([]byte(bci.currentHash))
      if bytes.Compare([]byte(bci.currentHash), []byte("")) == 0 {
         break
      }

      block := bci.Next()

      //fmt.Print("FindTransaction after: ")
      //fmt.Println([]byte(bci.currentHash))

      for _, tx := range block.Transactions {
         if bytes.Compare(tx.TransacID, tID) == 0 {
            return *tx, nil
         }

         HashToInt, _ := strconv.Atoi(string(block.PrevBlockHash))
         if HashToInt == 0 {
            break
         }
      }
   }
   return Transaction{}, errors.New("Transaction is not Found")
}

func (bc *Blockchain) SignTransaction(tx *Transaction, privKey ecdsa.PrivateKey) {
   prevTXs := make(map[string]Transaction)

   for _, vin := range tx.Vin {
      prevTX, err := bc.FindTransaction(vin.Txid)
      prevTXs[hex.EncodeToString(prevTX.TransacID)] = prevTX
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
      prevTXs[hex.EncodeToString(prevTX.TransacID)] = prevTX
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

func NewCoinBaseTX(to, data string) *Transaction {
   txin := TXInput{[]byte{}, -1, nil, []byte(data)}
   txout := NewTXOutput(subsidy, to)
   tx := Transaction{nil, []TXInput{txin}, []TXOutput{*txout}}
   tx.TransacID = tx.Hash()

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
            if /*out.used == false && */ out.IsLockedWithKey(pubKeyHash) {
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

         err := b.Put(tx.TransacID, newOutputs.Serialize())
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
   tx.TransacID = tx.Hash()

   node.utxoSet.blockchain.SignTransaction(&tx, wallet.PrivateKey)

   return &tx
}

//MerkleTree

type MerkleTree struct {
   RootNode *MerkleNode
}

type MerkleNode struct {
   Left  *MerkleNode
   Right *MerkleNode
   Data  []byte
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

//bucket

type ID string

type NodeBucket struct {
   lk   sync.RWMutex
   list *list.List
}

func newBucket() *NodeBucket {
   b := new(NodeBucket)
   b.list = list.New()
   return b
}

func (b *NodeBucket) Peers() []ID { //ID 배열 리턴
   b.lk.RLock()
   defer b.lk.RUnlock()
   ps := make([]ID, 0, b.list.Len())
   for e := b.list.Front(); e != nil; e = e.Next() {
      id := e.Value.(ID)
      ps = append(ps, id)
   }
   return ps
}

func (b *NodeBucket) Has(id ID) bool { //특정 ID의 존재 확인
   b.lk.RLock()
   defer b.lk.RUnlock()
   for e := b.list.Front(); e != nil; e = e.Next() {
      if e.Value.(ID) == id {
         return true
      }
   }
   return false
}

func (b *NodeBucket) Remove(id ID) { //특정 ID 제거
   b.lk.Lock()
   defer b.lk.Unlock()
   for e := b.list.Front(); e != nil; e = e.Next() {
      if e.Value.(ID) == id {
         b.list.Remove(e)
      }
   }
}

func (b *NodeBucket) MoveToFront(id ID) { //특정 ID의 위치 맨 앞으로 이동
   b.lk.Lock()
   defer b.lk.Unlock()
   for e := b.list.Front(); e != nil; e = e.Next() {
      if e.Value.(ID) == id {
         b.list.MoveToFront(e)
      }
   }
}

func (b *NodeBucket) PushFront(p ID) { //ID 맨 앞에 추가
   b.lk.Lock()
   b.list.PushFront(p)
   b.lk.Unlock()
}

func (b *NodeBucket) PopBack() ID { //ID 맨 뒤 제거
   b.lk.Lock()
   defer b.lk.Unlock()
   last := b.list.Back()
   b.list.Remove(last)
   return last.Value.(ID)
}

func (b *NodeBucket) Len() int { //Bucket의 element 개수 - O(1)
   b.lk.RLock()
   defer b.lk.RUnlock()
   return b.list.Len()
}

func (b *NodeBucket) Split(cpl int, target DHTID) *NodeBucket {
   b.lk.Lock()
   defer b.lk.Unlock()

   out := list.New()
   newbuck := newBucket()
   newbuck.list = out
   e := b.list.Front()
   for e != nil {
      peerID := ConvertPeerID(e.Value.(ID))
      peerCPL := commonPrefixLen(peerID, target)
      if peerCPL > cpl {
         cur := e
         out.PushBack(e.Value)
         e = e.Next()
         b.list.Remove(cur)
         continue
      }
      e = e.Next()
   }
   return newbuck
}

//keyspace

type Key struct { //Keyspace의 ID..?

   Space KeySpace //Key가 포함된 KeySpace

   Original []byte //identifier의 오리지널 값

   Bytes []byte //KeySpace안에 있는 identifier의 새로운 해싱값

}

func (k1 Key) Equal(k2 Key) bool { //두 키가 같은지 비교
   if k1.Space != k2.Space {
      panic("k1 and k2 not in same key space.")
   }
   return k1.Space.Equal(k1, k2)
}

func (k1 Key) Less(k2 Key) bool { //순서상으로 k1이 k2보다 앞서는지 확인
   if k1.Space != k2.Space {
      panic("k1 and k2 not in same key space.")
   }
   return k1.Space.Less(k1, k2)
}

func (k1 Key) Distance(k2 Key) *big.Int { //k1, k2의 xor distance 출력
   if k1.Space != k2.Space {
      panic("k1 and k2 not in same key space.")
   }
   return k1.Space.Distance(k1, k2)
}

type KeySpace interface {
   Key([]byte) Key //ID -> Key 전환

   Equal(Key, Key) bool

   Less(Key, Key) bool

   Distance(Key, Key) *big.Int
}

type byDistanceToCenter struct { //center에 가까운 Key들로 정렬하는데 사용
   Center Key
   Keys   []Key
}

func (s byDistanceToCenter) Len() int {
   return len(s.Keys)
}

func (s byDistanceToCenter) Swap(i, j int) {
   s.Keys[i], s.Keys[j] = s.Keys[j], s.Keys[i]
}

func (s byDistanceToCenter) Less(i, j int) bool {
   a := s.Center.Distance(s.Keys[i])
   b := s.Center.Distance(s.Keys[j])
   return a.Cmp(b) == -1
}

func SortByDistance(sp KeySpace, center Key, toSort []Key) []Key {
   toSortCopy := make([]Key, len(toSort))
   copy(toSortCopy, toSort)
   bdtc := &byDistanceToCenter{
      Center: center,
      Keys:   toSortCopy, // copy
   }
   sort.Sort(bdtc) //Quick Sort: O(nlogn)
   return bdtc.Keys
}

//XORKeySpace: ID를 SHA256으로 정규화, XOR distance 측정

var XORKeySpace = &xorKeySpace{}
var _ KeySpace = XORKeySpace //일치하는지 확인

type xorKeySpace struct{}

func (s *xorKeySpace) Key(id []byte) Key {
   hash := sha256.Sum256(id)
   key := hash[:]
   return Key{
      Space:    s,
      Original: id,
      Bytes:    key,
   }

}

func (s *xorKeySpace) Equal(k1, k2 Key) bool {
   return bytes.Equal(k1.Bytes, k2.Bytes)
}

func (s *xorKeySpace) Distance(k1, k2 Key) *big.Int {

   k3 := XOR(k1.Bytes, k2.Bytes) //XORing

   dist := big.NewInt(0).SetBytes(k3) //int로 변환
   return dist
}

func (s *xorKeySpace) Less(k1, k2 Key) bool {
   return bytes.Compare(k1.Bytes, k2.Bytes) < 0
}

//sorting

type peerDistance struct {
   p        ID
   distance DHTID
}

type peerSorterArr []*peerDistance //

func (p peerSorterArr) Len() int      { return len(p) }
func (p peerSorterArr) Swap(a, b int) { p[a], p[b] = p[b], p[a] }
func (p peerSorterArr) Less(a, b int) bool {
   return p[a].distance.less(p[b].distance)
}

func copyPeersFromList(target DHTID, peerArr peerSorterArr, peerList *list.List) peerSorterArr {

   if cap(peerArr) < len(peerArr)+peerList.Len() {
      newArr := make(peerSorterArr, 0, len(peerArr)+peerList.Len())
      copy(newArr, peerArr)
      peerArr = newArr
   }
   for e := peerList.Front(); e != nil; e = e.Next() {
      p := e.Value.(ID)
      pID := ConvertPeerID(p)
      pd := peerDistance{
         p:        p,
         distance: XOR(target, pID),
      }
      peerArr = append(peerArr, &pd)
   }
   return peerArr
}

func SortClosestPeers(peers []ID, target DHTID) []ID {
   psarr := make(peerSorterArr, 0, len(peers))
   for _, p := range peers {
      pID := ConvertPeerID(p)
      pd := &peerDistance{
         p:        p,
         distance: XOR(target, pID),
      }
      psarr = append(psarr, pd)
   }
   sort.Sort(psarr)
   out := make([]ID, 0, len(psarr))
   for _, p := range psarr {
      out = append(out, p.p)
   }
   return out
}

//rt

var LatencyEWMASmoothing = 0.1

//EWMA(변화의 속도)의 붕괴를 관리 - 정규화되어 있음: 0(변화X) ~ 1(100% 변화)

type Metrics interface { //peer들의 집합에서 xmetrics를 추적하는 개체

   RecordLatency(ID, time.Duration) //새로운 지속 시간을 측정하여 기록

   LatencyEWMA(ID) time.Duration //모든 피어들의 지속 시간의 EWMA를 측정하여 리턴
}

type xmetrics struct {
   latmap map[ID]time.Duration
   latmu  sync.RWMutex
}

func NewMetrics() *xmetrics {
   return &xmetrics{
      latmap: make(map[ID]time.Duration),
   }
}

func (m *xmetrics) RecordLatency(p ID, next time.Duration) {
   nextf := float64(next)
   s := LatencyEWMASmoothing
   if s > 1 || s < 0 {
      s = 0.1 // ignore the knob. it's broken. look, it jiggles.
   }

   m.latmu.Lock()
   ewma, found := m.latmap[p]
   ewmaf := float64(ewma)
   if !found {
      m.latmap[p] = next // when no data, just take it as the mean.
   } else {
      nextf = ((1.0 - s) * ewmaf) + (s * nextf)
      m.latmap[p] = time.Duration(nextf)
   }
   m.latmu.Unlock()
}

func (m *xmetrics) LatencyEWMA(p ID) time.Duration {
   m.latmu.RLock()
   lat := m.latmap[p]
   m.latmu.RUnlock()
   return time.Duration(lat)
}

//var log = logging.Logger("table")

type DHTID []byte

type RoutingTable struct {
   local DHTID //local peer의 ID

   tabLock sync.RWMutex //locking

   metrics Metrics //지속시간 계산

   maxLatency time.Duration //최대 지속시간

   Buckets    []*NodeBucket //kBuckets
   bucketsize int

/*
   refreshReq chan chan struct{}
   closed     chan struct{}
*/

   //알림 함수
   PeerRemoved func(ID)
   PeerAdded   func(ID)
}

func NewRoutingTable(bucketsize int, localID DHTID, latency time.Duration, m Metrics) *RoutingTable { //라우팅 테이블 생성
   rt := &RoutingTable{
      Buckets:     []*NodeBucket{newBucket()},
      bucketsize:  bucketsize,
      local:       localID,
      maxLatency:  latency,
      metrics:     m,
      /*
      refreshReq: make(chan chan struct{}),
      closed:     make(chan struct{}),
      */
      PeerRemoved: func(ID) {},
      PeerAdded:   func(ID) {},
   }

   return rt
}

/*
func (rt *RoutingTable) lookup(targetID ID, alpha int, refreshIfEmpty bool) []*Node {
   var (
      target         = ConvertPeerID(targetID)
      asked          = make(map[ID]bool)
      seen           = make(map[ID]bool)
      reply          = make(chan []*Node, alpha)
      pendingQueries = 0
      result         []*Node
   )
   // don't query further if we hit ourself.
   // unlikely to happen often in practice.
   asked[targetID] = true

   for {
      rt.tabLock.Lock()
      // generate initial result set
      result = rt.closest(target, k_of_bucket)
      rt.tabLock.Unlock()
      if len(result) > 0 || !refreshIfEmpty {
         break
      }
      // The result set is empty, all nodes were dropped, refresh.
      // We actually wait for the refresh to complete here. The very
      // first query will hit this case and run the bootstrapping
      // logic.
      <-rt.refresh()
      refreshIfEmpty = false
   }

   for {
      // ask the alpha closest nodes that we haven't asked yet
      for i := 0; i < len(result) && pendingQueries < alpha; i++ {
         n := result[i]
         if !asked[n.NodeID] {
            asked[n.NodeID] = true
            pendingQueries++
            go rt.findnode(n, targetID, reply)
         }
      }
      if pendingQueries == 0 {
         // we have asked all closest nodes, stop the search
         break
      }
      // wait for the next reply
      for _, n := range <-reply {
         if n != nil && !seen[n.NodeID] {
            seen[n.NodeID] = true
            result.push(n, k_of_bucket)
         }
      }
      pendingQueries--
   }
   return result
}

func (rt *RoutingTable) refresh() <-chan struct{} {
   done := make(chan struct{})
   select {
   case rt.refreshReq <- done:
   case <-rt.closed:
      close(done)
   }
   return done
}

func (rt *RoutingTable) closest(targetID ID, maxElems int) []*Node {
   // This is a very wasteful way to find the closest nodes but
   // obviously correct. I believe that tree-based buckets would make
   // this easier to implement efficiently.
   var close []*Node
   for _, b := range &rt.Buckets {
      for _, n := range b.lists {
         close = append(close, n)
      }
   }
   return close
}

func (rt *RoutingTable) findnode(n *Node, targetID ID, reply chan<- []*Node) {
   fails := tab.db.FindFails(n.ID())
   r, err := tab.net.findnode(n.ID(), n.addr(), targetKey)
   if err != nil || len(r) == 0 {
      fails++
      tab.db.UpdateFindFails(n.ID(), fails)
      log.Trace("Findnode failed", "id", n.ID(), "failcount", fails, "err", err)
      if fails >= maxFindnodeFailures {
         log.Trace("Too many findnode failures, dropping", "id", n.ID(), "failcount", fails)
         tab.delete(n)
      }
   } else if fails > 0 {
      tab.db.UpdateFindFails(n.ID(), fails-1)
   }

   // Grab as many nodes as possible. Some of them might not be alive anymore, but we'll
   // just remove those again during revalidation.
   for _, n := range r {
      tab.add(n)
   }
   reply <- r
}
*/

// Update adds or moves the given peer to the front of its respective bucket
// If a peer gets removed from a bucket, it is returned
func (rt *RoutingTable) Update(p ID) {
   peerID := ConvertPeerID(p)
   cpl := commonPrefixLen(peerID, rt.local)

   rt.tabLock.Lock()
   defer rt.tabLock.Unlock()
   bucketID := cpl
   if bucketID >= len(rt.Buckets) {
      bucketID = len(rt.Buckets) - 1
   }

   bucket := rt.Buckets[bucketID]
   if bucket.Has(p) {
      // If the peer is already in the table, move it to the front.
      // This signifies that it it "more active" and the less active nodes
      // Will as a result tend towards the back of the list
      bucket.MoveToFront(p)
      return
   }

   if rt.metrics.LatencyEWMA(p) > rt.maxLatency {
      // Connection doesnt meet requirements, skip!
      return
   }

   // New peer, add to bucket
   bucket.PushFront(p)
   rt.PeerAdded(p)

   // Are we past the max bucket size?
   if bucket.Len() > rt.bucketsize {
      // If this bucket is the rightmost bucket, and its full
      // we need to split it and create a new bucket
      if bucketID == len(rt.Buckets)-1 {
         rt.nextBucket()
      } else {
         // If the bucket cant split kick out least active node
         rt.PeerRemoved(bucket.PopBack())
      }
   }
}

// Remove deletes a peer from the routing table. This is to be used
// when we are sure a node has disconnected completely.
func (rt *RoutingTable) Remove(p ID) {
   rt.tabLock.Lock()
   defer rt.tabLock.Unlock()
   peerID := ConvertPeerID(p)
   cpl := commonPrefixLen(peerID, rt.local)

   bucketID := cpl
   if bucketID >= len(rt.Buckets) {
      bucketID = len(rt.Buckets) - 1
   }

   bucket := rt.Buckets[bucketID]
   bucket.Remove(p)
   rt.PeerRemoved(p)
}

func (rt *RoutingTable) nextBucket() {
   bucket := rt.Buckets[len(rt.Buckets)-1]
   newBucket := bucket.Split(len(rt.Buckets)-1, rt.local)
   rt.Buckets = append(rt.Buckets, newBucket)
   if newBucket.Len() > rt.bucketsize {
      rt.nextBucket()
   }

   // If all elements were on left side of split...
   if bucket.Len() > rt.bucketsize {
      rt.PeerRemoved(bucket.PopBack())
   }
}

// Find a specific peer by ID or return nil
func (rt *RoutingTable) Find(id ID) ID {
   srch := rt.NearestPeers(ConvertPeerID(id), 1)
   if len(srch) == 0 || srch[0] != id {
      return ""
   }
   return srch[0]
}

// NearestPeer returns a single peer that is nearest to the given ID
func (rt *RoutingTable) NearestPeer(id DHTID) ID {
   peers := rt.NearestPeers(id, 1)
   if len(peers) > 0 {
      return peers[0]
   }

   fmt.Println("NearestPeer: Returning nil, table size = %d", rt.Size())
   //log.Debugf("NearestPeer: Returning nil, table size = %d", rt.Size())
   return ""
}

// NearestPeers returns a list of the 'count' closest peers to the given ID
func (rt *RoutingTable) NearestPeers(id DHTID, count int) []ID {
   cpl := commonPrefixLen(id, rt.local)

   rt.tabLock.RLock()

   // Get bucket at cpl index or last bucket
   var bucket *NodeBucket
   if cpl >= len(rt.Buckets) {
      cpl = len(rt.Buckets) - 1
   }
   bucket = rt.Buckets[cpl]
   
   peerArr := make(peerSorterArr, 0, count)
   peerArr = copyPeersFromList(id, peerArr, bucket.list)
   if len(peerArr) < count {
      // In the case of an unusual split, one bucket may be short or empty.
      // if this happens, search both surrounding buckets for nearby peers
      if cpl > 0 {
         plist := rt.Buckets[cpl-1].list
         peerArr = copyPeersFromList(id, peerArr, plist)
      }

      if cpl < len(rt.Buckets)-1 {
         plist := rt.Buckets[cpl+1].list
         peerArr = copyPeersFromList(id, peerArr, plist)
      }
   }
   rt.tabLock.RUnlock()

   // Sort by distance to local peer
   sort.Sort(peerArr)

   if count < len(peerArr) {
      peerArr = peerArr[:count]
   }

   out := make([]ID, 0, len(peerArr))
   for _, p := range peerArr {
      out = append(out, p.p)
   }

   return out
}

// Size returns the total number of peers in the routing table
func (rt *RoutingTable) Size() int {
   var tot int
   rt.tabLock.RLock()
   for _, buck := range rt.Buckets {
      tot += buck.Len()
   }
   rt.tabLock.RUnlock()
   return tot
}

// ListPeers takes a RoutingTable and returns a list of all peers from all buckets in the table.
func (rt *RoutingTable) ListPeers() []ID {
   var peers []ID
   rt.tabLock.RLock()
   for _, buck := range rt.Buckets {
      peers = append(peers, buck.Peers()...)
   }
   rt.tabLock.RUnlock()
   return peers
}

// Print prints a descriptive statement about the provided RoutingTable
func (rt *RoutingTable) Print() {
   fmt.Printf("Routing Table, bs = %d, Max latency = %d\n", rt.bucketsize, rt.maxLatency)
   rt.tabLock.RLock()

   for i, b := range rt.Buckets {
      fmt.Printf("\tbucket: %d\n", i)

      b.lk.RLock()
      for e := b.list.Front(); e != nil; e = e.Next() {
         p := e.Value.(ID)
         fmt.Printf("\t\t- %s %s\n", p.Pretty(), rt.metrics.LatencyEWMA(p).String())
      }
      b.lk.RUnlock()
   }
   rt.tabLock.RUnlock()
}



//util

var ErrLookupFailure = errors.New("failed to find any peer in table")

func randSHA1ID() ID {
   //s1 := r.NewSource(time.Now().UnixNano())
   //r1 := r.New(s1)
   randn, _ := rand.Int(rand.Reader, big.NewInt(1 << (60 - 1)))
   randb := randn.Bytes()
   sha := sha1.Sum(randb)
   return ID(sha[:])
}

func (id DHTID) equal(other DHTID) bool {
   return bytes.Equal(id, other)
}

func (id DHTID) less(other DHTID) bool {
   a := Key{Space: XORKeySpace, Bytes: id}
   b := Key{Space: XORKeySpace, Bytes: other}
   return a.Less(b)
}

func commonPrefixLen(a, b DHTID) int {
   return ZeroPrefixLen(XOR(a, b))
}

func ZeroPrefixLen(id []byte) int { //byte slice의 연속된 0의 개수를 리턴
   for i, b := range id {
      if b != 0 {
         return i*8 + bits.LeadingZeros8(uint8(b))
      }
   }
   return len(id) * 8
}

func XOR(a, b DHTID) DHTID {
   return DHTID(byteXOR(a, b))
}

func byteXOR(a, b []byte) []byte {
   c := make([]byte, len(a))
   for i := 0; i < len(a); i++ {
      c[i] = a[i] ^ b[i]
   }
   return c
}


func ConvertPeerID(id ID) DHTID { //multihashing - create DHT ID(256bits) by hashing a Peer ID(160bits)
   hash := sha256.Sum256([]byte(id))
   return hash[:]
}

// Pretty returns a b58-encoded string of the ID
func (id ID) Pretty() string {
   return IDB58Encode(id)
}

// Alphabet is a a b58 alphabet.
type Alphabet struct {
   decode [128]int8
   encode [58]byte
}

// BTCAlphabet is the bitcoin base58 alphabet.
var BTCAlphabet = NewAlphabet("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

// FlickrAlphabet is the flickr base58 alphabet.
var FlickrAlphabet = NewAlphabet("123456789abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ")

// NewAlphabet creates a new alphabet from the passed string.
//
// It panics if the passed string is not 58 bytes long or isn't valid ASCII.
func NewAlphabet(s string) *Alphabet {
   if len(s) != 58 {
      panic("base58 alphabets must be 58 bytes long")
   }
   ret := new(Alphabet)
   copy(ret.encode[:], s)
   for i := range ret.decode {
      ret.decode[i] = -1
   }
   for i, b := range ret.encode {
      ret.decode[b] = int8(i)
   }
   return ret
}

// IDB58Encode returns b58-encoded string
func IDB58Encode(id ID) string {
   return Encode([]byte(id))
}

// Encode encodes the passed bytes into a base58 encoded string.
func Encode(bin []byte) string {
   return FastBase58EncodingAlphabet(bin, BTCAlphabet)
}

// FastBase58EncodingAlphabet encodes the passed bytes into a base58 encoded
// string with the passed alphabet.
func FastBase58EncodingAlphabet(bin []byte, alphabet *Alphabet) string {
   zero := alphabet.encode[0]

   binsz := len(bin)
   var i, j, zcount, high int
   var carry uint32

   for zcount < binsz && bin[zcount] == 0 {
      zcount++
   }

   size := (binsz-zcount)*138/100 + 1
   var buf = make([]uint32, size)

   high = size - 1
   for i = zcount; i < binsz; i++ {
      j = size - 1
      for carry = uint32(bin[i]); j > high || carry != 0; j-- {
         carry += buf[j] << 8
         buf[j] = carry % 58
         carry /= 58
      }
      high = j
   }

   for j = 0; j < size && buf[j] == 0; j++ {
   }

   var b58 = make([]byte, size-j+zcount)

   if zcount != 0 {
      for i = 0; i < zcount; i++ {
         b58[i] = zero
      }
   }

   for i = zcount; j < size; i++ {
      b58[i] = alphabet.encode[buf[j]]
      j++
   }

   return string(b58)
}

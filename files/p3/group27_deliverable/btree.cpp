/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, 
 * University of Wisconsin-Madison.
 */
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/bad_scanrange_exception.h"

namespace badgerdb {
//=============================================================================
//
// Private Helper Methods: The following are custom private helper methods.
//
//=============================================================================  
    /**
    * Helper method to check if the key is satisfied.
    * @param lowVal   Low value of range, pointer to integer / double / char string
    * @param lowOp    Low operator (GT/GTE)
    * @param highVal  High value of range, pointer to integer / double / char string
    * @param highOp   High operator (LT/LTE)
    * @param val      Value of the key
    * @return True if satisfied; False otherwise.
    */
    const bool BTreeIndex::is_key_satisfied(int lowVal, const Operator lowOp, 
      int highVal, const Operator highOp, int key) {
      //check each range one by one
      if(lowOp == GTE && highOp == LTE) return key <= highVal && key >= lowVal;
      else if(lowOp == GT && highOp == LTE) return key <= highVal && key > lowVal;
      else if(lowOp == GTE && highOp == LT) return key < highVal && key >= lowVal;
      else return key < highVal && key > lowVal;
    }
    /**
     * Find the next nonleaf position to be inserted.
     * @param cur_page      current page to be checked
     * @param next_pageid   return val for the next level pageid
     * @param key           the key to be checked
     */
    const void BTreeIndex::findnext_nonleaf(NonLeafNodeInt *cur_page, 
      PageId &next_pageid, int key) {
      int i = nodeOccupancy;//keep decrement until find the right pos
      for(;i >= 0 && (cur_page->pageNoArray[i]) == 0;i--);
      for(;i > 0 && (cur_page->keyArray[i-1]) >= key;i--);
      next_pageid = cur_page->pageNoArray[i];
    }
    /**
     * helper method to insert the index entry to the right position of the index file
     * @param cur_page      current page to be checked
     * @param cur_pid       current pageid
     * @param is_leaf       is leaf or not
     * @param target        actual index to be inserted
     * @param new_entry     A pageKeyPair:the entry that is pushed up after 
     *                      splitting a node, or null when no split in child node
     */
    const void BTreeIndex::insert(Page *cur_page, PageId cur_pid, bool is_leaf,
      const RIDKeyPair<int> target, PageKeyPair<int>*& new_entry) {
      if (is_leaf) {//trivial for leaf
        LeafNodeInt* leaf = (LeafNodeInt *)cur_page;
        if (leaf->ridArray[leafOccupancy - 1].page_number == 0) {
          insert_leaf(leaf, target);
          bufMgr->unPinPage(file, cur_pid, true);
          new_entry = nullptr;
        } else split_leaf(leaf, cur_pid, new_entry, target);
      } else {
        Page* next_page;
        PageId next_pageid;//init
        NonLeafNodeInt* cur_node = (NonLeafNodeInt*) cur_page;
        findnext_nonleaf(cur_node, next_pageid, target.key);
        bufMgr->readPage(file, next_pageid, next_page);
        is_leaf = cur_node->level == 1;
        insert(next_page, next_pageid, is_leaf, target, new_entry);
        if (new_entry == nullptr) bufMgr->unPinPage(file, cur_pid, false);
        else {
          if (cur_node->pageNoArray[nodeOccupancy] == 0) {//nonnull
            insert_nonleaf(cur_node, new_entry);
            new_entry = nullptr;
            bufMgr->unPinPage(file, cur_pid, true);//done, unpin page
          } else {
            split_nonleaf(cur_node, cur_pid, new_entry);
          }
        }
      }
    }
    /**
     * insert the index entry to the index file
     * @param tobe_split           the node to be split
     * @param old_pagenum        PageId of the node tobe_split
     * @param new_entry     A entry that is pushed up after splitting 
     */
    const void BTreeIndex::split_nonleaf(NonLeafNodeInt *tobe_split, 
      PageId pid, PageKeyPair<int> *&new_entry) {
      Page* new_page;
      PageId new_pid;//new nonleaf node init
      PageKeyPair<int> pushup_entry;//entry to be pushed up
      bufMgr->allocPage(file, new_pid, new_page);
      NonLeafNodeInt *new_node = (NonLeafNodeInt *) new_page;
      int pushup_index = nodeOccupancy/2, mid = nodeOccupancy/2;
      if (nodeOccupancy % 2 == 0) 
        pushup_index = new_entry->key < tobe_split->keyArray[mid] ? mid - 1 : mid;
      pushup_entry.set(new_pid, tobe_split->keyArray[pushup_index]);
      mid = pushup_index + 1;// move half the entries to the new node
      for(int i = mid; i < nodeOccupancy; i++) 
        new_node->keyArray[i - mid] = tobe_split->keyArray[i], 
          new_node->pageNoArray[i - mid] = tobe_split->pageNoArray[i + 1], 
          tobe_split->pageNoArray[i + 1] = (PageId) 0, 
          tobe_split->keyArray[i + 1] = 0;
      new_node->level = tobe_split->level,tobe_split->keyArray[pushup_index] = 0,
      tobe_split->pageNoArray[pushup_index] = (PageId) 0;
      // remove the entry that is pushed up from current node
      insert_nonleaf(new_entry->key < new_node->keyArray[0]
                     ? tobe_split : new_node, new_entry);
      new_entry = &pushup_entry;
      bufMgr->unPinPage(file, pid, true);
      bufMgr->unPinPage(file, new_pid, true);
      if (pid == rootPageNum)  update_root(pid, new_entry);
    }
    /**
     * For root that needs to be split, create a new root and insert the pushed-up 
     * entry and do the update.
     * @param firstpage_inroot   the pageid of the first pointer in the root page
     * @param new_entry     the entry that is pushed up
     */
    const void BTreeIndex::update_root(PageId firstpage_inroot, 
      PageKeyPair<int> *new_entry) {
      Page* new_root;
      PageId new_root_pid; 
      Page* temp;
      bufMgr->allocPage(file, new_root_pid, new_root);
      NonLeafNodeInt *new_root_page = (NonLeafNodeInt *)new_root;
      //update metadata
      new_root_page->level = init_rpn == rootPageNum ? 1 : 0,
        new_root_page->pageNoArray[0] = firstpage_inroot;
      new_root_page->pageNoArray[1] = new_entry->pageNo, 
        new_root_page->keyArray[0] = new_entry->key;
      bufMgr->readPage(file, headerPageNum, temp);
      IndexMetaInfo* meta_info = (IndexMetaInfo *) temp;
      meta_info->rootPageNo = new_root_pid, rootPageNum = new_root_pid;
      bufMgr->unPinPage(file, headerPageNum, true);
      bufMgr->unPinPage(file, new_root_pid, true);
    }
    /**
     * split leaf node that is full
     * @param full_node          full leaf node
     * @param num_leafpage   the number of page of that leaf
     * @param new_entry the entry to be pushed up
     * @param target     the entry to be inserted
     */
    const void BTreeIndex::split_leaf(LeafNodeInt *full_node, PageId num_leafpage, 
      PageKeyPair<int> *&new_entry, const RIDKeyPair<int> target) {
      Page *new_page;
      PageId new_pid; 
      PageKeyPair<int> newKeyPair; 
      bufMgr->allocPage(file, new_pid, new_page);
      LeafNodeInt *newLeafNode = (LeafNodeInt *)new_page;
      int mid = leafOccupancy/2;
      if (leafOccupancy %2 == 1 
        && target.key > full_node->keyArray[mid]) mid = mid + 1;//odd move ahead
      for(int i = mid; i < leafOccupancy; i++) {//copy half to new leaf node
        newLeafNode->keyArray[i-mid] = full_node->keyArray[i], 
          newLeafNode->ridArray[i-mid] = full_node->ridArray[i];
        full_node->keyArray[i] = 0, full_node->ridArray[i].page_number = 0;
      }
      if (target.key > full_node->keyArray[mid-1]) insert_leaf(newLeafNode, target);
      else insert_leaf(full_node, target);
      newLeafNode->rightSibPageNo = full_node->rightSibPageNo, 
        full_node->rightSibPageNo = new_pid, new_entry = new PageKeyPair<int>();
      //the smallest key from second page
      newKeyPair.set(new_pid, newLeafNode->keyArray[0]);
      new_entry = &newKeyPair;
      bufMgr->unPinPage(file, num_leafpage, true);
      bufMgr->unPinPage(file, new_pid, true);
      //curpage is root!
      if (num_leafpage == rootPageNum) update_root(num_leafpage, new_entry);
    }
    /**
     * insert an entry into a leaf node
     * @param leaf     leaf node that needs to be inserted into
     * @param entry    then entry needed to be inserted
     */
    const void BTreeIndex::insert_leaf(LeafNodeInt *leaf, RIDKeyPair<int> entry) {
      if (leaf->ridArray[0].page_number == 0) {//empty
        leaf->keyArray[0] = entry.key, 
          leaf->ridArray[0] = entry.rid;
      } else {
        int i = leafOccupancy - 1;
        for(;i >= 0 && (leaf->ridArray[i].page_number == 0);i--);
        for(;i >= 0 && (leaf->keyArray[i] > entry.key);i--) {
          leaf->keyArray[i+1] = leaf->keyArray[i],
            leaf->ridArray[i+1] = leaf->ridArray[i];
        }
        //do the work
        leaf->keyArray[i+1] = entry.key,leaf->ridArray[i+1] = entry.rid;
      }
    }
    /**
     * insert an entry into a nonleaf node
     * @param nonleaf  nonleaf node that need to be inserted into
     * @param entry    then entry needed to be inserted
     *
     */
    const void BTreeIndex::insert_nonleaf(NonLeafNodeInt *nonleaf, 
      PageKeyPair<int> *entry) {
      int i = nodeOccupancy;//keep decrement until find the right position
      for(;i >= 0 && (nonleaf->pageNoArray[i] == 0);i--);
      for(;i > 0 && (nonleaf->keyArray[i-1] > entry->key);i--) {
        nonleaf->keyArray[i] = nonleaf->keyArray[i-1],
          nonleaf->pageNoArray[i+1] = nonleaf->pageNoArray[i];
      }
      //do the work
      nonleaf->keyArray[i] = entry->key,nonleaf->pageNoArray[i+1] = entry->pageNo;
    }
//=============================================================================
//
// Public Methods: The following are methods provided from API.
//
//=============================================================================   
    /**
     * BTreeIndex Constructor. 
     * Check to see if the corresponding index file exists. If so, open the file.
     * If not, create it and insert entries for every tuple in the base relation
     * using FileScan class.
     *
     * @param relationName        Name of file.
     * @param outIndexName        Return the name of index file.
     * @param bufMgrIn            Buffer Manager Instance
     * @param attrByteOffset      Offset of attribute, over which index is to be 
     *                            built, in the record
     * @param attrType            Datatype of attribute over which index is built
     * @throws  BadIndexInfoException     If the index file already exists for the 
     * corresponding attribute, but values in metapage(relationName, attribute byte 
     * offset, attribute type etc.) do not match with values received through
     * constructor parameters.
     */
    BTreeIndex::BTreeIndex(const std::string & relationName,
            std::string & outIndexName,
            BufMgr *bufMgrIn,
            const int attrByteOffset,
            const Datatype attrType) {
      bufMgr = bufMgrIn, leafOccupancy = INTARRAYLEAFSIZE, 
      nodeOccupancy = INTARRAYNONLEAFSIZE, scanExecuting = false;
      std::ostringstream idxStr;//concat to get index ame
      idxStr << relationName << "." << attrByteOffset;
      outIndexName = idxStr.str();
      try {
        file = new BlobFile(outIndexName, false),
          headerPageNum = file->getFirstPageNo();
        Page *header_page;
        bufMgr->readPage(file, headerPageNum, header_page);
        IndexMetaInfo *meta_info = (IndexMetaInfo *)header_page;
        rootPageNum = meta_info->rootPageNo;//check if index info is valid
        if (relationName!=meta_info->relationName || 
          attrByteOffset!=meta_info->attrByteOffset || 
          attrType!=meta_info->attrType) throw BadIndexInfoException(outIndexName);
        bufMgr->unPinPage(file, headerPageNum, false);
      } catch(FileNotFoundException e) {
        Page *header_page;
        Page *root_page;
        RecordId rid;//not found file so open a new file 
        file = new BlobFile(outIndexName, true);
        bufMgr->allocPage(file, headerPageNum, header_page);
        bufMgr->allocPage(file, rootPageNum, root_page);
        IndexMetaInfo *meta_info = (IndexMetaInfo *)header_page;
        meta_info->attrByteOffset = attrByteOffset, 
          meta_info->attrType = attrType,
          meta_info->rootPageNo = rootPageNum, init_rpn = rootPageNum;
        strncpy((char *)(&(meta_info->relationName)), relationName.c_str(), 20);
        meta_info->relationName[19] = 0;//terminate str
        LeafNodeInt *root = (LeafNodeInt *)root_page;
        root->rightSibPageNo = 0;
        bufMgr->unPinPage(file, headerPageNum, true);
        bufMgr->unPinPage(file, rootPageNum, true);
        FileScan fileScan(relationName, bufMgr);
        try {
            while(1) {//scan everything
                fileScan.scanNext(rid);
                std::string record = fileScan.getRecord();
                insertEntry(record.c_str() + attrByteOffset, rid);
            }
        } catch (EndOfFileException e) { bufMgr->flushFile(file); }
      }
    }
    /**
     * BTreeIndex Destructor. 
     * End any initialized scan, flush index file, after unpinning any pinned 
     * pages, from the buffer manager
     * and delete file instance thereby closing the index file.
     * Destructor should not throw any exceptions. All exceptions should be 
     * caught in here itself. 
     * */
    BTreeIndex::~BTreeIndex() {
      scanExecuting = false;
      bufMgr->flushFile(BTreeIndex::file);
      delete file;
      file = nullptr;
    }
    /**
     * Insert a new entry using the pair <value,rid>. 
     * Start from root to recursively find out the leaf to insert the entry in.
     * The insertion may cause splitting of leaf node.
     * This splitting will require addition of new leaf page number entry into 
     * the parent non-leaf, which may in-turn get split.
     * This may continue all the way upto the root causing the root to get split. 
     * If root gets split, metapage needs to be changed accordingly.
     * Make sure to unpin pages as soon as you can.
     * @param key     Key to insert, pointer to integer/double/char string
     * @param rid     Record ID of a record whose entry is getting 
     * inserted into the index.
    **/
    const void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
      RIDKeyPair<int> entry;Page* root;
      entry.set(rid, *((int *)key));
      bufMgr->readPage(file, rootPageNum, root);
      PageKeyPair<int> *new_entry = nullptr;
      insert(root, rootPageNum, init_rpn == rootPageNum ? 
        true : false, entry, new_entry);
    }
    /**
     * Begin a filtered scan of the index.  For instance, if the method is called 
     * using ("a",GT,"d",LTE) then we should seek all entries with a value 
     * greater than "a" and less than or equal to "d".
     * If another scan is already executing, that needs to be ended here.
     * Set up all the variables for scan. Start from root to find out the 
     * leaf page that contains
     * the first RecordID
     * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
     * @param lowVal  Low value of range, pointer to integer / double / char string
     * @param lowOp   Low operator (GT/GTE)
     * @param highVal High value of range, pointer to integer / double / char string
     * @param highOp  High operator (LT/LTE)
     * @throws  BadOpcodesException If lowOp and highOp do not contain one of their 
     * their expected values 
     * @throws  BadScanrangeException If lowVal > highval
     * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that 
     * satisfies the scan criteria.
    **/
    const void BTreeIndex::startScan(const void* lowValParm,
               const Operator lowOpParm,
               const void* highValParm,
               const Operator highOpParm) {
      lowValInt = *((int *)lowValParm), highValInt = *((int *)highValParm);
      if(!((lowOpParm == GT or lowOpParm == GTE) and (highOpParm == LT 
        or highOpParm == LTE))) throw BadOpcodesException();
      if(lowValInt > highValInt) throw BadScanrangeException();
      lowOp = lowOpParm, highOp = highOpParm;
      if(scanExecuting) endScan();
      currentPageNum = rootPageNum;
      bufMgr->readPage(file, currentPageNum, currentPageData);
      //read root_page into the buffer pool
      if(init_rpn != rootPageNum) {//not leaf
        NonLeafNodeInt* curr = (NonLeafNodeInt *) currentPageData;
        bool found_leaf = false; 
        PageId next_page;
        while(!found_leaf) {
          curr = (NonLeafNodeInt *) currentPageData; 
          //if this is the level above the leaf, then the next level = leaf
          if(curr->level == 1) found_leaf = true;
          findnext_nonleaf(curr, next_page, lowValInt);
          bufMgr->unPinPage(file, currentPageNum, false);
          currentPageNum = next_page;
          bufMgr->readPage(file, currentPageNum, currentPageData);//read next
        }
      }
      bool found = false;//find the smallest one
      while(!found){
        LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
        if(currentNode->ridArray[0].page_number == 0) {//isnull
          bufMgr->unPinPage(file, currentPageNum, false);
          throw NoSuchKeyFoundException();
        }
        bool isnull = false;//traverse from l to r
        for(int i = 0; i < leafOccupancy and !isnull; i++) {
          int key = currentNode->keyArray[i];
          if(i < leafOccupancy - 1 
            and currentNode->ridArray[i + 1].page_number == 0) isnull = true;
          //Check if the next one in the key is not inserted
          if(is_key_satisfied(lowValInt, lowOp, highValInt, highOp, key)) {
            nextEntry = i, found = true, scanExecuting = true;
            break;
          } else if ((highOp == LT and key >= highValInt) 
            or (highOp == LTE and key > highValInt)) {
            bufMgr->unPinPage(file, currentPageNum, false);
            throw NoSuchKeyFoundException();
          }
          if(i == leafOccupancy - 1 or isnull) {//no match go to next leaf then
            bufMgr->unPinPage(file, currentPageNum, false);
            if(currentNode->rightSibPageNo == 0) throw NoSuchKeyFoundException();
            currentPageNum = currentNode->rightSibPageNo;
            bufMgr->readPage(file, currentPageNum, currentPageData);
          }
        }
      }
      }
    /**
     * Fetch the record id of the next index entry that matches the scan.
     * Return the next record from current page being scanned. 
     * If current page has been scanned to its entirety, move on to the 
     * right sibling of current page, if any exists, to start scanning that 
     * page. Make sure to unpin any pages that are no longer required.
     * @param outRid  RecordId of next record found that satisfies the scan 
     * criteria returned in this
     * @throws ScanNotInitializedException If no scan has been initialized.
     * @throws IndexScanCompletedException If no more records, satisfying the 
     * scan criteria, are left to be scanned.
    **/
    const void BTreeIndex::scanNext(RecordId& outRid) {
      if(!scanExecuting) throw ScanNotInitializedException();
      LeafNodeInt* currentNode = (LeafNodeInt *) currentPageData;
      if(currentNode->ridArray[nextEntry].page_number == 0 
        or nextEntry == leafOccupancy) {
        bufMgr->unPinPage(file, currentPageNum, false);
        if(currentNode->rightSibPageNo == 0) throw IndexScanCompletedException();
        currentPageNum = currentNode->rightSibPageNo;
        bufMgr->readPage(file, currentPageNum, currentPageData);
        currentNode = (LeafNodeInt *) currentPageData, nextEntry = 0;
      }
      int key = currentNode->keyArray[nextEntry];
      if(!is_key_satisfied(lowValInt, lowOp, highValInt, highOp, key)) 
        throw IndexScanCompletedException();
      else {
        outRid = currentNode->ridArray[nextEntry];
        nextEntry++; // current page has been fully scanned
      }
    }
    /**
     * Terminate the current scan. Unpin any pinned pages. 
     * Reset scan specific variables.
     * @throws ScanNotInitializedException If no scan has been initialized.
    **/
    const void BTreeIndex::endScan() {
      if(!scanExecuting) throw ScanNotInitializedException();
      scanExecuting = false;
      bufMgr->unPinPage(file, currentPageNum, false);
      currentPageData = nullptr,currentPageNum = static_cast<PageId>(-1),
        nextEntry = -1;//reset
    }
}

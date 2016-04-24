/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include <file_iterator.h>
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"
#include "exceptions/page_not_pinned_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{

	//TODO: UNPIN UNNEEDED PAGES THROUGH OUT THIS FUNCTION

    //create the filename
    std::ostringstream idxStr;
    idxStr << relationName << '.' << attrByteOffset;
    outIndexName = idxStr.str();
	
	//set values of the private variables
	bufMgr = bufMgrIn;
    attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
	scanExecuting = false;
	headerPageNum = 1;
	
	if(attrType == INTEGER) {
		leafOccupancy = INTARRAYLEAFSIZE;
		nodeOccupancy = INTARRAYNONLEAFSIZE;
	} else if(attrType == DOUBLE) {
		leafOccupancy = DOUBLEARRAYLEAFSIZE;
		nodeOccupancy = DOUBLEARRAYNONLEAFSIZE;
	} else if(attrType == STRING) {
		leafOccupancy = STRINGARRAYLEAFSIZE;
		nodeOccupancy = STRINGARRAYNONLEAFSIZE;
	} else {
		std::cout << "ERROR: non valid data type passed to BTreeIndex constructor\n";
	}

    //check if that file exists
	Page* metadataPage;
	IndexMetaInfo* metadata;
    try {
		file = new BlobFile(outIndexName, false);
	
		//if the file exists, read the first page which contains metadata information
		bufMgr->readPage(file, 1, metadataPage);
		metadata = (IndexMetaInfo*) metadataPage;
		
		//make sure the metadata matches whats passed in if the file already exists
		if(	metadata->attrType != attrType ||
			metadata->attrByteOffset != attrByteOffset ||
			strcmp(metadata->relationName, relationName.c_str()) != 0 ) {

			//if something doesnt match, then throw an exception
			throw BadIndexInfoException("");
		} 
		
		//set the root page for this index
		rootPageNum = metadata->rootPageNo;
		return;
	} catch(const FileExistsException &e) {
		
	}
	
	//if the code reaches here then the file didnt exist so we must create one
	file = new BlobFile(outIndexName, true);
	
	//make a metadata Page for this new index
	PageId metadataPageId;
	bufMgr->allocPage(file, metadataPageId, metadataPage);
	bufMgr->readPage(file, metadataPageId, metadataPage);
	metadata = (IndexMetaInfo*) metadataPage;
	
	//set variables in the metadata page
	strncpy(metadata->relationName, relationName.c_str(), 20);
	metadata->attrType = attrType;
	metadata->attrByteOffset = attrByteOffset;
	
	//create a new root page
	Page* rootPage;
	bufMgr->allocPage(file, rootPageNum /*private variable in this class. set here*/, rootPage);
	bufMgr->readPage(file, rootPageNum, rootPage);
	metadata->rootPageNo = rootPageNum;
	
	//the rootPage will become a non-leaf node
	switch(attrType) {
		case Datatype::INTEGER: {
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeInt* rootNode = (NonLeafNodeInt*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) rootNode->keyArray[i] = NULL;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty leaf page of the attribute type
			Page* leafPage; PageId leafPageId;
			bufMgr->allocPage(file, leafPageId, leafPage);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeInt* leafNode = (LeafNodeInt*) leafPage;

			//initialize the leaf page
			leafNode->rightSibPageNo = NULL;
			for(int i = 0; i < leafOccupancy; i++) leafNode->keyArray[i] = NULL;
			for(int i = 0; i < leafOccupancy; i++) {
				RecordId rid;
				rid.page_number = NULL;
				rid.slot_number = NULL;
				leafNode->ridArray[i] = rid;
			}

			rootNode->pageNoArray[0] = leafPageId;
			break;
		}
		case Datatype::DOUBLE: {
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeDouble* rootNode = (NonLeafNodeDouble*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++) rootNode->keyArray[i] = NULL;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty leaf page of the attribute type
			Page* leafPage; PageId leafPageId;
			bufMgr->allocPage(file, leafPageId, leafPage);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeDouble* leafNode = (LeafNodeDouble*) leafPage;

			//initialize the leaf page
			leafNode->rightSibPageNo = NULL;
			for(int i = 0; i < leafOccupancy; i++) leafNode->keyArray[i] = NULL;
			for(int i = 0; i < leafOccupancy; i++) {
				RecordId rid;
				rid.page_number = NULL;
				rid.slot_number = NULL;
				leafNode->ridArray[i] = rid;
			}

			rootNode->pageNoArray[0] = leafPageId;
			break;
		}
		case Datatype::STRING: {
			//initialize the rootNode with NULL key, pageNo pairs
			NonLeafNodeString* rootNode = (NonLeafNodeString*) rootPage;
			rootNode->level = 1;
			for(int i = 0; i < nodeOccupancy; i++)
				for(int j = 0; j < STRINGSIZE; j++)
					rootNode->keyArray[i][j] = NULL;
			for(int i = 0; i < nodeOccupancy + 1; i++) rootNode->pageNoArray[i] = NULL;

			//create an empty leaf page of the attribute type
			Page* leafPage; PageId leafPageId;
			bufMgr->allocPage(file, leafPageId, leafPage);
			bufMgr->readPage(file, leafPageId, leafPage);
			LeafNodeString* leafNode = (LeafNodeString*) leafPage;

			//initialize the leaf page
			leafNode->rightSibPageNo = NULL;
			for(int i = 0; i < leafOccupancy; i++)
				for(int j = 0; j < STRINGSIZE; j++)
					leafNode->keyArray[i][j] = NULL;
			for(int i = 0; i < leafOccupancy; i++) {
				RecordId rid;
				rid.page_number = NULL;
				rid.slot_number = NULL;
				leafNode->ridArray[i] = rid;
			}

			rootNode->pageNoArray[0] = leafPageId;
			break;
		}
		default: {};
	}

	

	//insert records from this relation into the tree
	//Create a file scanner for this relaion and buffer manager
	FileScan* fileScan = new FileScan(relationName, bufMgr);
	RecordId rid;
	const char* recordPtr;
	std::string record;
	try {
		//when we reach the end of this file, an exception will be thrown so we will exit then
		while(true) {
			fileScan->scanNext(rid);
			record = fileScan->getRecord();
			recordPtr = record.c_str();
			std::string key;

			switch(attrType) {
				case INTEGER:
					insertEntry((int*) (recordPtr + attrByteOffset), rid);
					break;
				case DOUBLE:
					insertEntry((double*) (recordPtr + attrByteOffset), rid);
					break;
				case STRING: 
					char keyBuf[STRINGSIZE];
					strncpy(keyBuf, (char*)(recordPtr + attrByteOffset), sizeof(keyBuf));
					key = std::string(keyBuf);
					insertEntry(&key, rid);
					break;
				default: ;
			}
		}
	} catch (EndOfFileException &e) {
		//end of the scan has been reached
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// Destructor. Method does not throw any exceptions as is indicated in the header file. All exceptions are caught in here itself.
	
	// TODO: Performing cleanup by clearing up state variables
	
	// Ending any initialized scan  and unpinning any B+ Tree pages that are pinned by invoking the endScan method.
	// endScan method can throw the ScanNotInitializedException and PageNotPinned (thrown by unPinPage) which are caught in here
	if(scanExecuting)
	{
		try {
			endScan();
		} catch(const ScanNotInitializedException &e) { 
			std::cout << "ScanNotInitializedException thrown in BtreeIndex destructor\n"; 
		}
	}
	
	// Flushing the index file from the buffer manager if it exists
	if(file) {
		bufMgr->flushFile(file);
	}
	
	// Deleting the file object instance. This automatically invokes the destructor of the File class and closes the index file.
	delete file;
	
	// TODO: Remember to clean up any state variables. Maybe state variables that we set up in the constructor? 
	// NOTE: The ~FileScan method (which also shuts down scan and unpins any pinned pages) sets the currentPage to null, clears the dirty bit and sets the file iterator 
	// to point to the beginning of the file. Not sure if that's what they mean by perform any cleanup. This is not done in any other version though.
	// NOTE: Printng out error messages for now. We can remove them if required.
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	//read in the root page. 
	Page* rootPage;
	bufMgr->readPage(file, rootPageNum, rootPage);

	//cast the rootPage to a non leaf node depending on type
	switch(attributeType) {
		case Datatype::INTEGER: {
			//iteratively search from the root
			NonLeafNodeInt* node = (NonLeafNodeInt*) rootPage;
			int keyValue = *((int*) key);
			int length = sizeof(node->keyArray) / sizeof(node->keyArray[0]);

			//keep traversing until level == 1. then we are one level above the leaves
			while(node->level == 0) {
				for(int i = 0; i < length; i++) {
					//if it's less than the first then take the value at 0
					if(keyValue < node->keyArray[0]) {
						Page* child; 
						bufMgr->readPage(file, node->pageNoArray[0], child);
						node = (NonLeafNodeInt*) child;
						break;
					}
					//if its between the current position and the next position and we are not at the end
					else if(keyValue > node->keyArray[i] && i != length && keyValue < node->keyArray[i+1]) {
						Page* child;
						bufMgr->readPage(file, node->pageNoArray[i+1], child);
						node = (NonLeafNodeInt*) child;
						break;
					}
					//if its greater than the last value
					else if(keyValue > node->keyArray[i] && (i == length || node->keyArray[i+1] == NULL)) {
						Page* child;
						bufMgr->readPage(file, node->pageNoArray[i+1], child);
						node = (NonLeafNodeInt*) child;
						break;
					}
				}
			}

			//at this point we know node is the parent of the page the rid is on so we need to search once more
			//find which page the data is on
			Page* leafPage;
			LeafNodeInt* leaf;
			
			//TODO: if key == any value in leaf node, BAD THINGS HAPPEN

			if(node->keyArray[0] == NULL) {
				bufMgr->readPage(file, node->pageNoArray[0], leafPage);
				leaf = (LeafNodeInt*) leafPage;
			} else {
				for(int i = 0; i < length; i++) {
					if(keyValue < node->keyArray[0]) {
						bufMgr->readPage(file, node->pageNoArray[0], leafPage);
						leaf = (LeafNodeInt*) leafPage;
						break;
					} else if(keyValue > node->keyArray[i] && i != length && keyValue < node->keyArray[i+1]) {
						bufMgr->readPage(file, node->pageNoArray[i+1], leafPage);
						leaf = (LeafNodeInt*) leafPage;
						break;
					} else if(keyValue > node->keyArray[i] && (i == length || node->keyArray[i+1] == NULL)) {
						bufMgr->readPage(file, node->pageNoArray[i+1], leafPage);
						leaf = (LeafNodeInt*) leafPage;
						break;
					}
				}
			}

			//leaf now points to the page where the record should be inserted
			//loop through the keys and find where this key goes
			if(leaf->keyArray[INTARRAYLEAFSIZE - 1] != NULL) {
				//just insert the record
			}
			else {
				//split the leaf page
				//then insert the 
			}

			break;
		}
		case Datatype::DOUBLE: {
			break;
		}
		case Datatype::STRING: {
			break;
		}
		default: {};
	}
	//node = rootPage
	
	/*
	PSEDUO CODE HERE
	while(node.level = 0) {
		for(i = 0; i < node.keyArray.length; i++) {
			
			if(key < node.keyArray[0]) node = pageNoArray[0]; break;
			else if(key > node.keyArray[i] && i != node.keyArray.length - 1 && key < node.keyArray[i+1]) node = pageNoArray[i+1]; break;
			else if(key > node.keyArray[i] && (i == node.keyArray.length -1 || node.keyArray[i+1] = INT_MAX)) node = pageNoArray[i+1]; break;
		}	
	}

	//now we know we are at the level above the page where we need to insert this (key, rid) tuple
	//once again find where we need to insert this tuple

	//make sure to cast the leaf node to the correct type
	for(i = 0; i < keyArray.length; i++) {
		if(key < node.keyArray[0]) node = pageNoArray[0]; break;
		else if(key > node.keyArray[i] && i != node.keyArray.length - 1 && key < node.keyArray[i+1]) node = pageNoArray[i+1]; break;
		else if(key > node.keyArray[i] && (i == node.keyArray.length -1 || node.keyArray[i+1] = INT_MAX)) node = pageNoArray[i+1]; break;
	}
	
	
	
	
	*/
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	// This method begins a filtered scan of the index 
	
	// Ending another scan that may be already executing. EndScan can throw a ScanNotInitializedException   
	if(scanExecuting){
		try {
			endScan();
		} catch(const ScanNotInitializedException &e) { 
			std::cout << "ScanNotInitializedException thrown in BtreeIndex startScan\n"; 
		}
	}
	
	// TODO: Perhaps clear these variables in the endScan or the destructor or both
	// Setting up operator member variables in BtreeIndex Class
	lowOp = lowOpParm;
	highOp = highOpParm;
	

	// lowOp must be either GT/GTE and highOp must be LT/LTE. BadOpCodesException is thrown if that is not the case.
    	if(     !((lowOp == GT)||(lowOp == GTE))  ||     !((highOp == LT)||(highOp == LTE))     ){
		throw BadOpcodesException();  
	}
	
	// TODO: Perhaps clear these variables in the endScan or the destructor or both
	// Setting up other scan related member variables in BTreeIndex Class
	nextEntry = 0;
	currentPageNum = rootPageNum; // Starting at the root
	Page* rootPage; // Reading in rootPage into buffer
	bufMgr->readPage(file, rootPageNum, rootPage);

	switch(attributeType){

		case Datatype::INTEGER:

			// TODO: Perhaps clear these variables in the endScan or the destructor or both
			//Dereferencing by casting void* to int*
			lowValInt =  *((int*)lowValParm); 
			highValInt = *((int*)highValParm);

			// Method throws exception if lower bound > upper bound
			if (lowValInt > highValInt){
				throw BadScanrangeException();
			}

			// Starting scan since the range and operators are valid
			scanExecuting = true;

			// Finding the leaf page with the first RecordID satisfying scan parameters

			// Iterative search starting at the root 
			NonLeafNodeInt* node = (NonLeafNodeInt*)rootPage;
			int length = sizeof(node->keyArray) / sizeof(node->keyArray[0]); // # of keys in the node

			// Traversing until we reach one level above the leaves (level=1)
			while (node->level == 0){

				// TODO/QUESTION: Can we just move this first condition lowVal < keyArray[0] outside the for loop because it doesnt depend on 'i' and use continue to check if we are one level above the leaf? We can change back if you want
				// Picking the correct child node by comparing lowVal to each key in this node
				// If lowVal < first key, then the page number at 0 is picked as child page
				if (lowValInt <= node->keyArray[0]) { // NOTE: Changed to <=
					Page* child;
					PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
					currentPageNum = node->pageNoArray[0]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
					bufMgr->readPage(file, node->pageNoArray[0], child);
					node = (NonLeafNodeInt*)child;
					bufMgr->unPinPage( file, lastPageNum, false ); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
					continue; 	
				}
				// If lowVal !< first key, we check other keys in this node
				for (int i = 0; i < length; i++) {
					//If its between the current position and the next position and we are not at the end
					if (lowValInt > node->keyArray[i] && i != length && lowValInt <= node->keyArray[i + 1]) { // NOTE: Changed to <=
						Page* child;
						PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						currentPageNum = node->pageNoArray[i+1]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						bufMgr->readPage(file, node->pageNoArray[i + 1], child);
						node = (NonLeafNodeInt*)child;
						bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						break;
					}
					//If its greater than the last value
					else if (lowValInt > node->keyArray[i] && (i == length || node->keyArray[i + 1] == NULL)) { //TODO/QUESTION: Should we not have (i == length || i = INTARRAYNONLEAFSIZE) instead? Is length = INTARRAYNONLEAFSIZE/ is it equal to the number of keys actually present?
						Page* child;
						PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						currentPageNum = node->pageNoArray[i + 1]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						bufMgr->readPage(file, node->pageNoArray[i + 1], child);
						node = (NonLeafNodeInt*)child;
						bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						break;
					}
				}

			}

			//At this point we know node is the parent of the page the rid is on so we need to search once more
			//to find which page with the first RecordID satisfying scan parameters
			Page* leafPage;
			LeafNodeInt* leaf;

			//TODO: if key == any value in leaf node, BAD THINGS HAPPEN // Please check note below
			if (node->keyArray[0] == NULL) {
				PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
				currentPageNum = node->pageNoArray[0]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
				bufMgr->readPage(file, node->pageNoArray[0], leafPage);
				leaf = (LeafNodeInt*)leafPage;
				bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
			}
			else {
				for (int i = 0; i < length; i++) {
					if (lowValInt <= node->keyArray[0]) { //NOTE: Changed to <=
						PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						currentPageNum = node->pageNoArray[0]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						bufMgr->readPage(file, node->pageNoArray[0], leafPage);
						leaf = (LeafNodeInt*)leafPage;
						bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						break;
					}
					else if (lowValInt > node->keyArray[i] && i != length && lowValInt <= node->keyArray[i + 1]) { //NOTE: Changed to <=
						PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						currentPageNum = node->pageNoArray[i+1]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						bufMgr->readPage(file, node->pageNoArray[i + 1], leafPage);
						leaf = (LeafNodeInt*)leafPage;
						bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						break;
					}
					else if (lowValInt > node->keyArray[i] && (i == length || node->keyArray[i + 1] == NULL)) { //TODO/QUESTION: Should we not have (i == length || i = INTARRAYNONLEAFSIZE) instead? Is length = INTARRAYNONLEAFSIZE or is it equal to the number of keys actually present?
						PageId lastPageNum = currentPageNum; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						currentPageNum = node->pageNoArray[i+1]; // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						bufMgr->readPage(file, node->pageNoArray[i + 1], leafPage);
						leaf = (LeafNodeInt*)leafPage;
						bufMgr->unPinPage(file, lastPageNum, false); // TODO/QUESTION: Related to unpinPage. Please check if you agree.
						break;
					}
				}
			}

			//Leaf now points to the page where the first recordID may be found
			
			//////// FILL IN THE BLANKS ///////////////
			break;

		case Datatype::DOUBLE:
			//Dereferencing by casting void* to double* 
			lowValDouble  = *((double*)lowValParm); 
			highValDouble = *((double*)highValParm);
			// Method throws exception if lower bound > upper bound
			if (lowValDouble > highValDouble){
				throw BadScanrangeException();
			}
			// Starting scan since the range and operators are valid
			scanExecuting = true;
			// Finding the leaf page with the first RecordID satisfying scan parameters
			
			//////// FILL IN THE BLANKS ///////////////
			break;

		case Datatype::STRING: 
			//Initializing to empty string
			//Dereferencing by casting void* to char*
			//Using first 10 chars of the char array for string comparison
			lowValString.clear(); 
			for (int i = 0; i < 10; i++){
				lowValString += *((char*)lowValParm + i); 
			}
			highValString.clear(); 
			for (int i = 0; i < 10; i++){
				highValString += *((char*)highValParm + i);
			}
			// Method throws exception if lower bound > upper bound
			if (lowValString > highValString){
				throw BadScanrangeException();
			}
			// Starting scan since the range and operators are valid
			scanExecuting = true;
			// Finding the leaf page with the first RecordID satisfying scan parameters
			
			//////// FILL IN THE BLANKS ///////////////
			break;

		default: break;
	}	
	

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan() 
{
	// Method terminates the current scan and  throws a ScanNotInitializedException if invoked before a succesful startScan call
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	else
	{
		scanExecuting = false;
	}

	// Unpinning all the pages that have been pinned for the purpose of scan
	try {
		bufMgr->unPinPage(file, currentPageNum, false);
	} catch(const PageNotPinnedException &e) {
		std::cout << "PageNotPinned thrown in endScan()\n";
	}
	// TODO: Should the dirty bit be set to false irrespective of whether the page is actually dirty or not?
	// TODO: Check if the currentPage is the only page pinned for the purpose of the scan 
}

}

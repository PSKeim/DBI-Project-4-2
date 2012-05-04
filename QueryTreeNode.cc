#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "QueryTreeNode.h"
#include <iostream>
#include <vector>

using namespace std;
/*
using std::clog;
using std::endl;
using std::vector;
*/

QueryTreeNode::QueryTreeNode(){

  lChildPipeID = 0;
  rChildPipeID = 0;
  outPipeID = 0;

  parent = NULL;
  left = NULL;
  right = NULL;

  attsToKeep = NULL;
  numAttsToKeep = NULL;

  cnf = NULL;
  schema = NULL;
  order = NULL;
  funcOp = NULL;
  func = NULL;

}


QueryTreeNode::~QueryTreeNode(){

}


QueryNodeType 	QueryTreeNode::GetType(){
  return type;
}

std::string QueryTreeNode::GetTypeName(){

  string name;

  switch (type){

    case SELECTF:
      name = "SELECT FILE";
      break;

    case SELECTP:
      name = "SELECT PIPE";
      break;

    case PROJECT:
      name = "PROJECT";
      break;

    case JOIN:
      name = "JOIN";
      break;

    case SUM:
      name = "SUM";
      break;

    case GROUP_BY:
      name = "GROUP BY";
      break;

    case DISTINCT:
      name = "DISTINCT";
      break;
  } // end switch

  return name;

} // end GetTypeName


/*

PRINT FUNCTIONS

*/
void QueryTreeNode::PrintInOrder(){
	if(NULL != left){
		left->PrintInOrder();
	}
	if(NULL != right){
		right->PrintInOrder();
	}
	PrintNode();
}

void QueryTreeNode::PrintNode(){
  clog << " *********** " << endl;
  clog << GetTypeName() << " operation" << endl;

  switch (type){

    case SELECTF:

	//clog << "SELECT FILE OPERATION" << endl;
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	PrintCNF();
      break;

    case SELECTP:
	//clog << "SELECT PIPE OPERATION" << endl;
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << "SELECTION CNF :" << endl;
	PrintCNF();
      break;

    case PROJECT:
	clog << "INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE "<< outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	clog << "************" << endl;
//      PrintCNF();
      break;

    case JOIN:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "RIGHT INPUT PIPE " << rChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	PrintCNF();
      break;

    case SUM:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;
	schema->Print();
	//PrintFunction();
      break;

    case DISTINCT:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "OUTPUT SCHEMA: " << endl;	
	schema->Print();
	break;

    case GROUP_BY:
	clog << "LEFT INPUT PIPE " << lChildPipeID << endl;
	clog << "OUTPUT PIPE " << outPipeID << endl;
	clog << "GROUPING ON " << endl;
	order->Print();
	clog << endl << "FUNCTION " << endl;
      break;

  } // end switch type

} // end Print


void QueryTreeNode::PrintCNF(){

  if (cnf){

    struct AndList *curAnd = cnf;
    struct OrList *curOr;
    struct ComparisonOp *curOp;

      // Cycle through the ANDs
    while (curAnd){

      curOr = curAnd->left;

      if (curAnd->left){
	clog << "(";
      }

	// Cycle through the ORs
      while (curOr){

	curOp = curOr->left;

	  // If op is not null, then parse
	if (curOp){

	  if (curOp->left){
	    clog << curOp->left->value;
	  }

	  //clog << endl << "CUR OP CODE IS " << curOp->code << endl;
	  switch (curOp->code){
	    case 5:
	      clog << " < ";
	      break;

	    case 6:
	      clog << " > ";
	      break;

	    case 7:
	      clog << " = ";
	      break;

	  } // end switch curOp->code

	  if (curOp->right){
	    clog << curOp->right->value;
	  }


	} // end if curOp

	if (curOr->rightOr){
	  clog << " OR ";
	}

	curOr = curOr->rightOr;

      } // end while curOr

      if (curAnd->left){
	clog << ")";
      }

      if (curAnd->rightAnd) {
	clog << " AND ";
      }

      curAnd = curAnd->rightAnd;

    } // end while curAnd

  } // end if parseTree
	clog << endl;

}


/*
GENERATOR FUNCTIONS (SCHEMA & FUNCTION & GROUP BY)
*/

void QueryTreeNode::GenerateSchema(){
	//Here, the schema is basically a shoved together version of the previous two schemas
	//clog << "Attempting to generate schema" << endl;
	Schema *lChildSchema = left->schema;
	Schema *rChildSchema = right->schema;
	
	schema = new Schema(lChildSchema, rChildSchema);
}

void QueryTreeNode::GenerateFunction(){

	if(type == SUM){
		clog << "Attempting to grow parse tree " << endl;
		func = new Function();
		clog << "Main thing I'm looking for is at: " << funcOp->leftOperand->value << endl;

		func->GrowFromParseTree(funcOp, *schema);
	}
	else{ //Only other type that should use this is Group By

		

	}
}

void QueryTreeNode::GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes){
			/*
				char *str_sum = "(ps_supplycost)";
				get_cnf (str_sum, &join_sch, func);
				func.Print ();
				OrderMaker grp_order;
				grp_order.numAtts = 1;
				grp_order.whichAtts[0] = 3;
				grp_order.whichTypes[0] = Int;
		*/

		order = new OrderMaker();
		
		order->numAtts = numAtts;
		for(int i = 0; i < whichAtts.size(); i++){
			order->whichAtts[i] = whichAtts[i];
			order->whichTypes[i] = (Type)whichTypes[i];
		}
		
		//cout << "GENERATED " << endl;
		//order->Print();
}

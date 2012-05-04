#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#include "Schema.h"
#include "Function.h"
#include "Comparison.h"

#include <vector>


enum QueryNodeType {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT};

class QueryTreeNode {

 public:

	      QueryNodeType type;
	      QueryTreeNode *parent;
	      QueryTreeNode *left;
	      QueryTreeNode *right;

	      //Pipe identifiers ( can be replaced by actual pipes later, or something)
		int lChildPipeID;
		int rChildPipeID;
		int outPipeID;

	      QueryTreeNode();
	 	~QueryTreeNode();

		void PrintInOrder();
	      void PrintNode ();
	      void PrintCNF();
		void PrintFunction();

	      string GetTypeName ();
	      QueryNodeType GetType ();
		

		//Used for JOIN
		void GenerateSchema();
		//Used for SUM
		void GenerateFunction();
		//Used for GROUP_BY
		void GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes);
		

// private:

		// For a PROJECT
		int numAttsIn;
		int numAttsOut;
	  	int *attsToKeep;
	  	int numAttsToKeep;
		//NameList *projectAtts;
		// For various operations
	      AndList *cnf;
	      Schema *schema;
		// For GROUP BY
	      OrderMaker *order;
		// For aggregate
	      FuncOperator *funcOp;
		Function *func;



	      vector<string> relations;

};

#endif

#include <iostream>
#include <occi.h>
#include <stdio.h>
#include <stdlib.h>

using namespace oracle::occi;
using namespace std;

class occipool 
{
  private:

  Environment *env;
  Connection *con;
  Statement *stmt;
  public :

  occipool(){
    env = Environment::createEnvironment (Environment::DEFAULT);
  }
  
  ~occipool(){
    Environment::terminateEnvironment(env);
  } 

  void refresh_schema(){
    system("sqlplus kester/oracle1234 @/home/mkester/harvard/pequod/db/hn/oracle-schema-drop.sql");
    system("sqlplus kester/oracle1234 @/home/mkester/harvard/pequod/db/hn/oracle-schema.sql");
  }

  bool import_db(){
    system("sqlplus kester/oracle1234 @/home/mkester/harvard/pequod/db/hn/oracle-schema-drop.sql");
    system("impdp kester/oracle1234 dumpfile=hn-schema.dmp logfile=impschema.log SCHEMAS=KESTER");

//    const string username = "kester";
//    const string passWord = "oracle1234";
//    Connection* const con = env->createConnection(username, passWord);
//    try{
//      if(con)
//        ; //worked
//      else
//        return false;
//    } catch (SQLException e){
//      cerr << "Can’t connect: " << e.what();
//      return false;
//    }
//
//    stmt = con->createStatement("commit");
//    stmt->executeQuery();
//
//
//    env->terminateConnection(con);
    return true;
  }

  void run(){
    cout << "occipool - Selecting records using ConnectionPool interface" <<
    endl;
    const string poolUserName = "kester";
    const string poolPassword = "oracle1234";
    const string connectString = "";
    const string username = "kester";
    const string passWord = "oracle1234";
    unsigned int maxConn = 150;
    unsigned int minConn = 75;
    unsigned int incrConn = 2;
    ConnectionPool *connPool = env->createConnectionPool
      (poolUserName, poolPassword, connectString, minConn, maxConn, incrConn);
    try{
    if (connPool)
      cout << "SUCCESS - createConnectionPool" << endl;
    else
      cerr << "FAILURE - createConnectionPool" << endl;
    con = connPool->createConnection (username, passWord);
    if (con)
      cout << "SUCCESS - createConnection" << endl;
    else
      cerr << "FAILURE - createConnection" << endl;
    }catch(SQLException ex)
    {
     cout<<"Exception thrown for createConnectionPool"<<endl;
     cout<<"Error number: "<<  ex.getErrorCode() << endl;
     cout<<ex.getMessage() << endl;
    }
    
  

//    stmt = con->createStatement 
 //     ("@/home/mkester/harvard/pequod/db/hn/oracle-schema.sql");
  //  stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt = con->createStatement 
//      ("DROP TABLE :1 CASCADE");
//    stmt->executeQuery();
//    stmt->setString(1, "karma_mv");
//    stmt->executeQuery();
//    stmt->setString(1, "karma");
//    stmt->executeQuery();
//    stmt->setString(1, "karma_v");
//    stmt->executeQuery();
//    stmt->setString(1, "articles");
//    stmt->executeQuery();
//    stmt->setString(1, "comments");
//    stmt->executeQuery();
//    stmt->setString(1, "votes");
//    stmt->executeQuery();

//DROP TABLE IF EXISTS karma_mv CASCADE;
//DROP TABLE IF EXISTS karma CASCADE;
//DROP VIEW IF EXISTS karma_v CASCADE;
//
//DROP TABLE IF EXISTS articles CASCADE;
//
//    stmt = con->createStatement 
//      ("INSERT INTO menu VALUES(:1,:2)");
//    stmt->setString(1, "Banana");
//    stmt->setString(2, "BEERS");
//    ResultSet *rset = stmt->executeQuery();
//    while (rset->next())
//    {
//      cout << "food:" << rset->getString (1) << endl;
//      cout << "drink:" << rset->getString (2) << endl;
//    }
//    stmt->closeResultSet (rset);
    con->terminateStatement (stmt);
    connPool->terminateConnection (con);
    env->terminateConnectionPool (connPool);

    cout << "occipool - done" << endl;
  } 

}; // end of class occipool

int main(void){

  string user = "";
  string passwd = "";
  string db = "";

  occipool *demo = new occipool();

  bool ok = demo->import_db();
  if(!ok)
    cerr << "Import failed aborting." << endl;

  delete demo;
 
  return 0;

}


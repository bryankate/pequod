#include <iostream>
#include <pqxx/pqxx>
#include <stdlib.h>

using namespace std;

int main(int, char *argv[])
{
  pqxx::connection c("dbname=twitter user=mkester");
  //pqxx::work txn(c);
  pqxx::nontransaction instant(c);

  pqxx::result r = instant.exec("DROP TABLE IF EXISTS users, posts, timeline");

  if (system("psql -d twitter < twit.dmp")){
    cerr << "FAIL: DB import went poorly." << endl;
    exit(EXIT_FAILURE);
  }
  instant.exec("CREATE TABLE posts (usrid integer, tweet char(140), occurred timestamp)");
  instant.exec("CREATE TABLE timeline (usrid integer, followid integer, tweet char(140), occurred timestamp)");
  instant.exec("CREATE INDEX posts_usrid on posts (usrid)");
  instant.exec("CREATE INDEX posts_occurred on posts (occurred)");
  instant.exec("CREATE INDEX timeline_occurred on timeline (occurred)");
  instant.exec("CREATE INDEX timeline_usrid on timeline (usrid)");
  instant.exec("DROP FUNCTION IF EXISTS pushtweets() CASCADE");

  instant.exec("CREATE FUNCTION pushtweets() RETURNS TRIGGER AS $$ BEGIN INSERT INTO timeline SELECT u.usr, NEW.* FROM users u WHERE u.follows = NEW.usrid; RETURN NULL; END; $$ LANGUAGE plpgsql");

  instant.exec("DROP TRIGGER IF EXISTS pushthetweets ON posts");
  instant.exec("CREATE TRIGGER pushthetweets AFTER INSERT ON posts FOR EACH ROW EXECUTE PROCEDURE pushtweets()");

//  r = txn.exec(
//    "SELECT * "
//    "FROM users "
//    "WHERE usr = follows");
//
//  for (int i = 0; i < r.size(); ++i){
//      cout << r[i][0].as<int>() << ":" << r[i][1].as<int>() << endl;    
//  }
//  cout << "we found " << r.size() << endl;
  
//  if (r.size() != 1)
//  {
//    std::cerr
//      << "Expected 1 employee with name " << argv[1] << ", "
//      << "but found " << r.size() << std::endl;
//    return 1;
//  }
//
//  int employee_id = r[0][0].as<int>();
//  std::cout << "Updating employee #" << employee_id << std::endl;
//
//  txn.exec(
//    "UPDATE EMPLOYEE "
//    "SET salary = salary + 1 "
//    "WHERE id = " + txn.quote(employee_id));
//
//  txn.commit();
}



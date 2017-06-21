package org.apache.zeppelin.realm;

import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.realm.jdbc.JdbcRealm;
import org.apache.shiro.util.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * Created by ctontfu@gmail.com on 2017/6/16.
 */
public class ZeppelinJdbcRealm extends JdbcRealm {
  private static final Logger log = LoggerFactory.getLogger(ZeppelinJdbcRealm.class);

  public ZeppelinJdbcRealm() {
    super();
  }

  public Set<String> getUserRoles(String username) {
    Connection conn = null;
    try {
      conn = dataSource.getConnection();
      Set<String> roleNames = getRoleNamesForUser(conn, username);
      return roleNames;
    } catch (SQLException e) {
      String message = "There was a SQL error while authorizing user [" + username + "]";
      if (log.isErrorEnabled()) {
        log.error(message, e);
      }
      throw new AuthorizationException(message, e);
    } finally {
      JdbcUtils.closeConnection(conn);
    }
  }

}

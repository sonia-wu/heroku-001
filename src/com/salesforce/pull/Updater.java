package com.salesforce.pull;

import java.net.Proxy;
import java.util.Arrays;
import java.util.Calendar;
import java.util.concurrent.TimeUnit;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.GetUpdatedResult;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

public class Updater {

	private static PartnerConnection conn;

	public static void main(String[] args) {
		if (args.length != 4) {
			System.err.println("Usage: url username password entityName");
			System.exit(1);
		}

		try {
			conn = initPartnerConnConfig(args[0], args[1], args[2]);
		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}

		final String entityName = args[3];
		try {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(System.currentTimeMillis());
			cal.add(Calendar.MINUTE, -1);
			Calendar oneMinBack = cal;
			//keep going 1 hour
			int count = 0;
			do {
				Calendar current = Calendar.getInstance();
				current.setTimeInMillis(System.currentTimeMillis());
				GetUpdatedResult updated = conn.getUpdated(entityName, oneMinBack, current);
				String[] ids = updated.getIds();
				System.out.println("Last min updated: " + Arrays.asList(ids) + " till " + current.getTime());
				count ++;
				try {
					TimeUnit.MINUTES.sleep(1);
				} catch (InterruptedException e) {
					//let go
				}
				oneMinBack = current;
			} while (count < 60);

		} catch (ConnectionException e) {
			throw new RuntimeException(e);
		}

	}

	private static PartnerConnection initPartnerConnConfig(String endpoint, String user, String pwd) throws ConnectionException {
		ConnectorConfig config = new ConnectorConfig();
		String apiEndPoint = endpoint + "/services/Soap/u/" + getDefaultApiVer();
		config.setAuthEndpoint(apiEndPoint);//make sure endpoint is sth like: https://na1-blitz04.soma.salesforce.com/services/Soap/u/37.0
		config.setServiceEndpoint(apiEndPoint);
		config.setUsername(user);
		config.setPassword(pwd);
		config.setManualLogin(true);
		config.setProxy(Proxy.NO_PROXY);
		config.setConnectionTimeout(/*we want many many sec ^_^ - 5 min*/5 * 60 * 1000);
		PartnerConnection pConn = Connector.newConnection(config);
		LoginResult loginResult = pConn.login(user, pwd);
		String sid = loginResult.getSessionId();
		pConn.setSessionHeader(sid);
		config.setServiceEndpoint(loginResult.getServerUrl());
		return pConn;

	}
	
	private static String getDefaultApiVer() {
		return "37.0";
	}

}

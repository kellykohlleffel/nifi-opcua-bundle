package com.kentender.nifi.nifi_opcua_services;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.opcfoundation.ua.transport.security.Cert;
import org.opcfoundation.ua.transport.security.KeyPair;
import org.opcfoundation.ua.transport.security.PrivKey;
import org.opcfoundation.ua.transport.security.SecurityPolicy;
import org.opcfoundation.ua.utils.CertificateUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
	
		private static final Logger logger = LoggerFactory.getLogger(Utils.class);
		private static String PRIVKEY_PASSWORD = "Opc.Ua";
	
	 	public static KeyPair getCert(String applicationName) {
	    	//create a key pair - I have changed the original .pem extension to .key
	  		return getCert(applicationName, SecurityPolicy.NONE);
		}
		
	    public static KeyPair getCert(String applicationName, org.opcfoundation.ua.transport.security.SecurityPolicy securityPolicy) {
	    	//create a key pair - I have changed the original .pem extension to .key
	  		return getCert(applicationName, applicationName + ".der", applicationName + ".key", securityPolicy);
		}

	    public static KeyPair getCert(String applicationName, String cert, String key, org.opcfoundation.ua.transport.security.SecurityPolicy securityPolicy) {
			
			File certFile = new File(cert);
			File privKeyFile =  new File(key);
			
			try {
				Cert myServerCertificate = Cert.load( certFile );
				PrivKey myServerPrivateKey = PrivKey.load( privKeyFile, PRIVKEY_PASSWORD );
				return new KeyPair(myServerCertificate, myServerPrivateKey); 
			} catch (CertificateException e) {
				logger.debug("Error while loading certificate ");
				logger.error(e.getMessage());
			} catch (NoSuchAlgorithmException e) {
				logger.error(e.getMessage());
			} catch (InvalidKeyException | InvalidParameterSpecException | BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | NoSuchPaddingException | InvalidKeySpecException e) {
				logger.error("Issue in getCert " + e.getMessage());
			} catch (IOException e) {
				try {
					String hostName = InetAddress.getLocalHost().getHostName();
					String applicationUri = "urn:"+hostName+":"+applicationName;
					/**
					 * Define the algorithm to use for certificate signatures.
					 * <p>
					 * The OPC UA specification defines that the algorithm should be (at least)
					 * "SHA1WithRSA" for application instance certificates used for security
					 * policies Basic128Rsa15 and Basic256. For Basic256Sha256 it should be
					 * "SHA256WithRSA".
					 * <p>
					 */
					
					if(securityPolicy == SecurityPolicy.BASIC128RSA15){
						CertificateUtils.setKeySize(1024);
						CertificateUtils.setCertificateSignatureAlgorithm("SHA1WithRSA");
					} else if(securityPolicy == SecurityPolicy.BASIC256) {
						CertificateUtils.setKeySize(2028);
						CertificateUtils.setCertificateSignatureAlgorithm("Basic256");
					} else if(securityPolicy == SecurityPolicy.BASIC256SHA256){
						CertificateUtils.setKeySize(2028);
						CertificateUtils.setCertificateSignatureAlgorithm("SHA256WithRSA");
					}

					KeyPair keys = CertificateUtils.createApplicationInstanceCertificate(applicationName, null, applicationUri, 3650, hostName);
					keys.getCertificate().save(certFile);
					keys.getPrivateKey().save(privKeyFile);
					
					return keys;
					
				} catch (Exception ex) {
					logger.error(ex.getMessage());
				}
			}
			return null;
	}
	    
		public static KeyPair getHttpsCert(String applicationName){
			File certFile = new File(applicationName + "_https.der");
			File privKeyFile =  new File(applicationName+ "_https.pem");
			try {
				Cert myServerCertificate = Cert.load( certFile );
				PrivKey myServerPrivateKey = PrivKey.load( privKeyFile, PRIVKEY_PASSWORD );
				return new KeyPair(myServerCertificate, myServerPrivateKey); 
			} catch (CertificateException | InvalidKeyException | NoSuchAlgorithmException e) {
				
				logger.error(e.getMessage());
			} catch (InvalidKeySpecException | InvalidParameterSpecException | BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | NoSuchPaddingException e) {
				// TODO Auto-generated catch block
				
				e.printStackTrace();
			} catch (IOException e) {

				try {
					KeyPair caCert = getCACert();
					String hostName = InetAddress.getLocalHost().getHostName();
					String applicationUri = "urn:"+hostName+":"+applicationName;
					KeyPair keys = CertificateUtils.createHttpsCertificate(hostName, applicationUri, 3650, caCert);
					keys.save(certFile, privKeyFile, PRIVKEY_PASSWORD);
					return keys;
				} catch (Exception e1) {
					logger.error(e1.getMessage());
				}
			}
			return null;
		}
		
		public static KeyPair getCACert(){
			File certFile = new File("NifiCA.der");
			File privKeyFile =  new File("NifiCA.pem");
			try {
				Cert myServerCertificate = Cert.load( certFile );
				PrivKey myServerPrivateKey = PrivKey.load( privKeyFile, PRIVKEY_PASSWORD );
				return new KeyPair(myServerCertificate, myServerPrivateKey); 
			} catch (CertificateException | NoSuchAlgorithmException e) {
				logger.error(e.getMessage());
			} catch (IOException e) {		
				try {
					KeyPair keys = CertificateUtils.createIssuerCertificate("NifiCA", 3650, null);
					keys.getCertificate().save(certFile);
					keys.getPrivateKey().save(privKeyFile, PRIVKEY_PASSWORD);
					return keys;
				} catch (Exception e1) {
					logger.error(e1.getMessage());
				}
			} catch (InvalidKeyException | InvalidKeySpecException | InvalidAlgorithmParameterException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException | InvalidParameterSpecException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;
		}

}
package example.hive.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class EncryptAndDecrypt {
//	PrivateKey privateKey;
//	PublicKey publicKey;
//	public DecryptOPENSSL (){
//		KeyPairGenerator generator;
//		try {
//			generator = KeyPairGenerator.getInstance("RSA");
//			generator.initialize(4098);
//			KeyPair pair = generator.generateKeyPair();
//			privateKey = pair.getPrivate();
//			publicKey = pair.getPublic();
//		} catch (NoSuchAlgorithmException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public byte[] encryptByRSA(String publicPEMPath, String input, String outputPath) {
		try {
			Cipher encryptCipher = Cipher.getInstance("RSA");
			PublicKey publicKey = getPublicPem(publicPEMPath);
			encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey);
			byte[] secretMessageBytes = input.getBytes(StandardCharsets.UTF_8);
			byte[] encryptedMessageBytes = encryptCipher.doFinal(secretMessageBytes);
//			String encodedMessage = Base64.getEncoder().encodeToString(encryptedMessageBytes);
			if(StringUtils.isNotEmpty(outputPath))
				FileUtils.writeByteArrayToFile(new File(outputPath), encryptedMessageBytes);
			return encryptedMessageBytes;
		} catch (IOException | InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException
				| InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public String decryptByRSA(String privatePEMPath, byte[] input) {
		try {
			Cipher decryptCipher = Cipher.getInstance("RSA");
			PrivateKey privateKey = getPrivatePem(privatePEMPath);
			decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
			byte[] decryptedMessageBytes = decryptCipher.doFinal(input);
			String decryptedMessage = new String(decryptedMessageBytes, StandardCharsets.UTF_8);
			return decryptedMessage;
		} catch (InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
				| IllegalBlockSizeException | BadPaddingException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void decryptByRSA(String privatePEMPath, String inputPath, String outputPath) {
		try {
			Cipher decryptCipher = Cipher.getInstance("RSA");
			PrivateKey privateKey = getPrivatePem(privatePEMPath);
			decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
			File file = new File(inputPath);
			byte[] bData = FileUtils.readFileToByteArray(file);
			byte[] decryptedMessageBytes = decryptCipher.doFinal(bData);
			if (StringUtils.isNotEmpty(outputPath) && decryptedMessageBytes != null)
				FileUtils.writeByteArrayToFile(new File(outputPath), decryptedMessageBytes);
		} catch (InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
				| IllegalBlockSizeException | BadPaddingException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String loadKey(String path) throws IOException {
		String strKeyPEM = "";
	    BufferedReader br = new BufferedReader(new FileReader(path));
	    String line;
	    while ((line = br.readLine()) != null) {
	        strKeyPEM += line + "\n";
	    }
	    br.close();
	    return strKeyPEM;
	}
	
	public PublicKey getPublicPem(String path) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		String publicKeyPEM = loadKey(path);
	    publicKeyPEM = publicKeyPEM.replace("-----BEGIN PUBLIC KEY-----\n", "");
	    publicKeyPEM = publicKeyPEM.replace("-----END PUBLIC KEY-----", "");
	    byte[] encoded = Base64.decodeBase64(publicKeyPEM);
	    KeyFactory kf = KeyFactory.getInstance("RSA");
	    PublicKey pubKey = kf.generatePublic(new X509EncodedKeySpec(encoded));
//		System.out.println("public= "+java.util.Base64.getEncoder().encodeToString(pubKey.getEncoded()));
	    return pubKey;
	}

	// command: openssl pkcs8 -topk8 -inform PEM -outform PEM -in priv1.pem -out private8.pem -nocrypt
	// convert -----BEGIN RSA PRIVATE KEY----- to -----BEGIN PRIVATE KEY-----
	public PrivateKey getPrivatePem(String path) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		String privateKeyPEM = loadKey(path);
		privateKeyPEM = privateKeyPEM.replace("-----BEGIN PRIVATE KEY-----\n", "");
	    privateKeyPEM = privateKeyPEM.replace("-----END PRIVATE KEY-----", "");
	    byte[] encoded = Base64.decodeBase64(privateKeyPEM);
	    KeyFactory kf = KeyFactory.getInstance("RSA");
	    PrivateKey privKey = kf.generatePrivate(new PKCS8EncodedKeySpec(encoded));
//		System.out.println("private= "+java.util.Base64.getEncoder().encodeToString(privKey.getEncoded()));
	    return privKey;
	}
	
	public byte[] enryptSymmetric(String privatePEMPath, String inputPath, String outputPath) {
		return null;
	}	
	public byte[] decryptSymmetric(String privatePEMPath, String inputPath, String outputPath) {
		return null;
	}
	
	public static void main(String[] args) {
		String test = "Baeldung secret message";
		EncryptAndDecrypt obj = new EncryptAndDecrypt();
		byte[] encryptedBytes = obj.encryptByRSA("/Users/xiujun/workspace/test/public.pem", test, null);
		String decryptedStr = obj.decryptByRSA("/Users/xiujun/workspace/test/private8.pem", encryptedBytes);
		System.out.println(decryptedStr);
	}

}

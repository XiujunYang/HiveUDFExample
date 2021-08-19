package example.hive.udf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import static java.nio.charset.StandardCharsets.UTF_8;

public class EncryptAndDecrypt {
	
	public String encryptByRSA(String publicPEMPath, String inputPath, String outputPath) {
		try {
			Cipher encryptCipher = Cipher.getInstance("RSA");
			PublicKey publicKey = getPublicPem(publicPEMPath);
			encryptCipher.init(Cipher.ENCRYPT_MODE, publicKey);
			byte[] secretMessageBytes = loadFile(inputPath).getBytes(StandardCharsets.UTF_8);
			byte[] encryptedMessageBytes = encryptCipher.doFinal(secretMessageBytes);
			if(StringUtils.isNotEmpty(outputPath)) FileUtils.writeByteArrayToFile(new File(outputPath), encryptedMessageBytes);
		} catch (IOException | InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException
				| InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return outputPath;
	}

	public String decryptByRSA(String privatePEMPath, String inputPath, String outputPath) {
		try {
			Cipher decryptCipher = Cipher.getInstance("RSA");
			PrivateKey privateKey = getPrivatePem(privatePEMPath);
			decryptCipher.init(Cipher.DECRYPT_MODE, privateKey);
			byte[] bData = FileUtils.readFileToByteArray(new File(inputPath));
			byte[] decryptedMessageBytes = decryptCipher.doFinal(bData);
			String decryptedMessage = new String(decryptedMessageBytes, StandardCharsets.UTF_8);
			if (StringUtils.isNotEmpty(outputPath) && decryptedMessageBytes != null)
				FileUtils.writeByteArrayToFile(new File(outputPath), decryptedMessageBytes);
			return decryptedMessage;
		} catch (InvalidKeySpecException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException
				| IllegalBlockSizeException | BadPaddingException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private String loadFile(String path) throws IOException {
		String strKeyPEM = "";
	    BufferedReader br = new BufferedReader(new FileReader(path));
	    String line;
	    while ((line = br.readLine()) != null) {
	        strKeyPEM += line + "\n";
	    }
	    br.close();
	    return strKeyPEM;
	}
	
	private PublicKey getPublicPem(String path) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		String publicKeyPEM = loadFile(path);
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
	private PrivateKey getPrivatePem(String path) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
		String privateKeyPEM = loadFile(path);
		privateKeyPEM = privateKeyPEM.replace("-----BEGIN PRIVATE KEY-----\n", "");
	    privateKeyPEM = privateKeyPEM.replace("-----END PRIVATE KEY-----", "");
	    byte[] encoded = Base64.decodeBase64(privateKeyPEM);
	    KeyFactory kf = KeyFactory.getInstance("RSA");
	    PrivateKey privKey = kf.generatePrivate(new PKCS8EncodedKeySpec(encoded));
	    return privKey;
	}
	
	//Symmetric encryption
	public String encryptfileByAesCbc(String passwordPath, String inputPath, String outputPath) {
        try {
            FileInputStream fis = new FileInputStream(inputPath);
            FileOutputStream fos = new FileOutputStream(outputPath);
            final byte[] pass = loadFile(passwordPath).replaceAll("\n", "").getBytes();
            final byte[] salt = (new SecureRandom()).generateSeed(8);
            fos.write("Salted__".getBytes(StandardCharsets.UTF_8));
            fos.write(salt);
            final byte[] passAndSalt = concatenateByteArrays(pass, salt);
            byte[] hash = new byte[0];
            byte[] keyAndIv = new byte[0];
            for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
                final byte[] hashData = concatenateByteArrays(hash, passAndSalt);
                final MessageDigest md = MessageDigest.getInstance("MD5");
                hash = md.digest(hashData);
                keyAndIv = concatenateByteArrays(keyAndIv, hash);
            }
            final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
            final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);
            final SecretKeySpec key = new SecretKeySpec(keyValue, "AES");
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
            CipherOutputStream cos = new CipherOutputStream(fos, cipher);
            int b;
            byte[] d = new byte[8];
            while ((b = fis.read(d)) != -1) {
                cos.write(d, 0, b);
            }
            cos.flush();
            cos.close();
            fis.close();
        } catch (IOException | NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        }
        return outputPath;
    }
	
	//Symmetric Decryption
    public String decryptFileByAesCbc(String passwordPath, String intputPath, String outputPath) {
        byte[] SALTED_MAGIC = "Salted__".getBytes(UTF_8);
        try{
            FileOutputStream fos = new FileOutputStream(outputPath);
            final byte[] pass = loadFile(passwordPath).replaceAll("\n", "").getBytes();//.getBytes(US_ASCII);
            final byte[] inBytes = Files.readAllBytes(Paths.get(intputPath));
            final byte[] shouldBeMagic = Arrays.copyOfRange(inBytes, 0, SALTED_MAGIC.length);
            if (!Arrays.equals(shouldBeMagic, SALTED_MAGIC)) {
                throw new IllegalArgumentException("Initial bytes from input do not match OpenSSL SALTED_MAGIC salt value.");
            }
            final byte[] salt = Arrays.copyOfRange(inBytes, SALTED_MAGIC.length, SALTED_MAGIC.length + 8);
            final byte[] passAndSalt = concatenateByteArrays(pass, salt);
            byte[] hash = new byte[0];
            byte[] keyAndIv = new byte[0];
            for (int i = 0; i < 3 && keyAndIv.length < 48; i++) {
                final byte[] hashData = concatenateByteArrays(hash, passAndSalt);
                MessageDigest md = MessageDigest.getInstance("MD5");
                hash = md.digest(hashData);
                keyAndIv = concatenateByteArrays(keyAndIv, hash);
            }
            final byte[] keyValue = Arrays.copyOfRange(keyAndIv, 0, 32);
            final SecretKeySpec key = new SecretKeySpec(keyValue, "AES");
            final byte[] iv = Arrays.copyOfRange(keyAndIv, 32, 48);
            final Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
            final byte[] clear = cipher.doFinal(inBytes, 16, inBytes.length - 16);
            String contentDecoded = new String(clear, StandardCharsets.UTF_8);
            fos.write(contentDecoded.getBytes());
            fos.flush();
            fos.close();
            return contentDecoded;
        }catch(Exception e){
            e.printStackTrace();
            return null;
        }
    }

    private byte[] concatenateByteArrays(byte[] a, byte[] b) {
        return ByteBuffer
                .allocate(a.length + b.length)
                .put(a).put(b)
                .array();
    }
	
	public static void main(String[] args) throws IOException {
		EncryptAndDecrypt obj = new EncryptAndDecrypt();
		String testFile = "/Users/xiujun/workspace/test/key";
		String filePath = obj.encryptByRSA("/Users/xiujun/workspace/test/public.pem", testFile, "/Users/xiujun/workspace/test/key.enc2");
		String decryptedStr = obj.decryptByRSA("/Users/xiujun/workspace/test/private8.pem", filePath, "/Users/xiujun/workspace/test/key2");
		System.out.println(decryptedStr);
		
		String file = obj.encryptfileByAesCbc("/Users/xiujun/workspace/test/key", "/Users/xiujun/workspace/test/test.json", "/Users/xiujun/workspace/test/test.enc2.json");
		String decryptedtext = obj.decryptFileByAesCbc("/Users/xiujun/workspace/test/key", file, "/Users/xiujun/workspace/test/test2.json");
		System.out.println(decryptedtext);
	}

}

/*
 * Copyright (C) 2026 John Garner
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.pikatimer.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.prefs.Preferences;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.pikatimer.PikaPreferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The self-signed certificate for the HTTPS port.
 *
 * Phone browsers only allow camera access from a secure page, so the select
 * timer web app has to be served over HTTPS. There is no internet at most race
 * sites, so we make our own certificate. Each device has to accept the browser
 * warning once.
 *
 * The certificate is kept in the user's preferences and reused, so a device
 * that accepted it before does not get asked again. A new one is made when it
 * is about to expire or when this computer has an IP address the certificate
 * does not cover.
 *
 * iOS refuses (with no way to proceed) certificates that do not follow Apple's
 * TLS rules, so the certificate has a SAN with every address, the serverAuth
 * extended key usage, and is valid for less than 825 days.
 *
 * @author John Garner
 */
public final class SelfSignedCertificate {
    private static final Logger logger = LoggerFactory.getLogger(SelfSignedCertificate.class);

    private static final String PREF_KEYSTORE = "HTTPS:keystore";
    private static final String PREF_PASSWORD = "HTTPS:keystorePassword";

    public static final String ALIAS = "pikatimer";
    private static final int VALID_DAYS = 800;
    private static final int RENEW_DAYS = 30;
    // Keep the SAN list from growing forever as the computer moves between networks
    private static final int MAX_ADDRESSES = 24;

    private final KeyStore keyStore;
    private final String password;
    private final X509Certificate certificate;

    private SelfSignedCertificate(KeyStore keyStore, String password, X509Certificate certificate) {
        this.keyStore = keyStore;
        this.password = password;
        this.certificate = certificate;
    }

    /**
     * Load the saved certificate, or make a new one if there isn't one, it is
     * about to expire, or it does not cover all of the given IP addresses.
     *
     * @param addresses this computer's current IP addresses
     * @return the certificate to use
     * @throws Exception if a certificate could not be made
     */
    public static synchronized SelfSignedCertificate load(Collection<String> addresses) throws Exception {
        SelfSignedCertificate saved = loadSaved();
        if (saved != null && !saved.expiresSoon() && saved.covers(addresses)) {
            logger.debug("Using the saved HTTPS certificate {}", saved.getFingerprint());
            return saved;
        }

        // Keep the addresses the old certificate covered, newest first, so
        // a device that is still using one of them doesn't need a new address
        Set<String> all = new LinkedHashSet<>(addresses);
        if (saved != null) all.addAll(saved.getAddresses());
        List<String> trimmed = new ArrayList<>(all);
        if (trimmed.size() > MAX_ADDRESSES) trimmed = trimmed.subList(0, MAX_ADDRESSES);

        SelfSignedCertificate fresh = generate(trimmed);
        fresh.save();
        logger.info("Created a new HTTPS certificate {} for {}", fresh.getFingerprint(), trimmed);
        return fresh;
    }

    private static SelfSignedCertificate loadSaved() {
        Preferences prefs = PikaPreferences.getInstance().getGlobalPreferences();
        String encoded = prefs.get(PREF_KEYSTORE, "");
        String password = prefs.get(PREF_PASSWORD, "");
        if (encoded.isEmpty() || password.isEmpty()) return null;
        try {
            KeyStore ks = KeyStore.getInstance("PKCS12");
            ks.load(new ByteArrayInputStream(Base64.getDecoder().decode(encoded)), password.toCharArray());
            Certificate cert = ks.getCertificate(ALIAS);
            if (!(cert instanceof X509Certificate x509) || ks.getKey(ALIAS, password.toCharArray()) == null) return null;
            return new SelfSignedCertificate(ks, password, x509);
        } catch (Exception ex) {
            logger.warn("Unable to load the saved HTTPS certificate, making a new one", ex);
            return null;
        }
    }

    private void save() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        keyStore.store(out, password.toCharArray());
        Preferences prefs = PikaPreferences.getInstance().getGlobalPreferences();
        prefs.put(PREF_KEYSTORE, Base64.getEncoder().encodeToString(out.toByteArray()));
        prefs.put(PREF_PASSWORD, password);
        prefs.flush();
    }

    private static SelfSignedCertificate generate(List<String> addresses) throws Exception {
        // EC P-256 keeps the keystore small enough to live in the preferences
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new ECGenParameterSpec("secp256r1"));
        KeyPair keyPair = kpg.generateKeyPair();

        SecureRandom random = new SecureRandom();
        X500Name name = new X500Name("CN=PikaTimer,O=PikaTimer");
        BigInteger serial = new BigInteger(63, random).add(BigInteger.ONE);
        Instant now = Instant.now();
        Date notBefore = Date.from(now.minus(1, ChronoUnit.DAYS));
        Date notAfter = Date.from(now.plus(VALID_DAYS, ChronoUnit.DAYS));

        List<GeneralName> names = new ArrayList<>();
        names.add(new GeneralName(GeneralName.dNSName, "localhost"));
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (host != null && !host.isBlank() && host.matches("[A-Za-z0-9.-]+")) {
                names.add(new GeneralName(GeneralName.dNSName, host));
                if (!host.contains(".")) names.add(new GeneralName(GeneralName.dNSName, host + ".local"));
            }
        } catch (Exception ex) {
            logger.debug("No host name for the certificate", ex);
        }
        names.add(new GeneralName(GeneralName.iPAddress, "127.0.0.1"));
        for (String address : addresses) {
            if (!"127.0.0.1".equals(address)) names.add(new GeneralName(GeneralName.iPAddress, address));
        }

        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
                name, serial, notBefore, notAfter, name, keyPair.getPublic());
        JcaX509ExtensionUtils extUtils = new JcaX509ExtensionUtils();
        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature));
        builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));
        builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(names.toArray(GeneralName[]::new)));
        builder.addExtension(Extension.subjectKeyIdentifier, false, extUtils.createSubjectKeyIdentifier(keyPair.getPublic()));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withECDSA").build(keyPair.getPrivate());
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(builder.build(signer));

        byte[] pw = new byte[18];
        random.nextBytes(pw);
        String password = Base64.getUrlEncoder().withoutPadding().encodeToString(pw);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, null);
        PrivateKey key = keyPair.getPrivate();
        ks.setKeyEntry(ALIAS, key, password.toCharArray(), new Certificate[]{cert});
        return new SelfSignedCertificate(ks, password, cert);
    }

    private boolean expiresSoon() {
        return certificate.getNotAfter().toInstant().isBefore(Instant.now().plus(RENEW_DAYS, ChronoUnit.DAYS));
    }

    /**
     * @param addresses IP addresses
     * @return true if the certificate covers all of them
     */
    public boolean covers(Collection<String> addresses) {
        return getAddresses().containsAll(addresses);
    }

    /**
     * @return the IP addresses in the certificate's SAN
     */
    public List<String> getAddresses() {
        List<String> result = new ArrayList<>();
        try {
            Collection<List<?>> sans = certificate.getSubjectAlternativeNames();
            if (sans != null) {
                for (List<?> san : sans) {
                    if (san.size() >= 2 && Integer.valueOf(GeneralName.iPAddress).equals(san.get(0))) {
                        result.add(String.valueOf(san.get(1)));
                    }
                }
            }
        } catch (Exception ex) {
            logger.debug("Unable to read the certificate's SAN", ex);
        }
        return result;
    }

    public KeyStore getKeyStore() {
        return keyStore;
    }

    public String getPassword() {
        return password;
    }

    public X509Certificate getCertificate() {
        return certificate;
    }

    /**
     * @return the SHA-256 fingerprint, as shown by browsers
     */
    public String getFingerprint() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
            return HexFormat.ofDelimiter(":").withUpperCase().formatHex(digest);
        } catch (Exception ex) {
            return "";
        }
    }
}

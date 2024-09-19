## AWS RDS

https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesAllRegions

https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem.

## Azure

https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/how-to-connect-tls-ssl
https://learn.microsoft.com/en-us/azure/postgresql/single-server/concepts-certificate-rotation

Includes:

- BaltimoreCyberTrustRoot
- DigiCertGlobalRootG2 Root CA
- Microsoft RSA Root Certificate Authority 2017
- Microsoft ECC Root Certificate Authority 2017
- DigiCert Global Root G3
- DigiCert Global Root CA

## Supabase

Downloaded from Supabase web portal, expires 2031.

## GCP Cloud SQL for PostgreSQL

Does not appear to have a global CA.

## ISRG Root X1

This one is for Let's Encrypt, used by some providers such as nhost.

Using the cross-signed root does not work for some reason, but the self-signed one works.

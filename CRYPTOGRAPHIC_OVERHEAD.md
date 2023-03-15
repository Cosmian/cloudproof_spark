# Cryptographic overhead

The cryptographic overhead is du to:

- **asymmetric encryption**, done by [CoverCrypt](#covercrypt-cryptographic-overhead) where implementation details are written [here](https://github.com/Cosmian/cover_crypt). As a reminder, `CoverCrypt` is used in `Parquet` encryption as a Key Encapsulation Mechanism (KEM).
- **symmetric encryption**, done (generally) by *AES256-GCM* algorithm through `Parquet` format. Parquet encryption overhead is described [here](https://github.com/apache/parquet-format/blob/master/Encryption.md#6-encryption-overhead).

<!-- toc -->

- [Quick summary](#quick-summary)
- [CoverCrypt cryptographic overhead](#covercrypt-cryptographic-overhead)
- [Examples](#examples)
  * [Encrypted footer mode only](#encrypted-footer-mode-only)
  * [Files and columns encryption](#files-and-columns-encryption)
  * [Post-quantum Resistant Policy](#post-quantum-resistant-policy)

<!-- tocstop -->

## Quick summary

Without post-quantum resistance, `CoverCrypt` scheme overhead size is equivalent to classic symmetric encryption overhead like *AES256-GCM* but with a hybrid cryptographic system with [multiple benefits](https://docs.cosmian.com/cloudproof_encryption/use_cases_benefits/).

## CoverCrypt cryptographic overhead

The **asymmetric encryption** is handled by `CoverCrypt` KEM. For each file and each sensitive column of the input dataset, symmetric keys are encapsulated and saved into `.parquet` files.

The following formula gives the overall encryption overhead of an arbitrary given dataset in `Parquet` format:

$$\text{overhead}_\text{total} = \text{number of encrypted files} \times \text{overhead}_\text{file}$$

where:

$$\text{overhead}_\text{file} =  \text{overhead}_{\text{file footer mode}} + \text{number of encrypted columns} \times \text{overhead}_{\text{column}} $$

<br/>

Both $overhead_\text{file footer mode}$ and $\text{overhead}_\text{column}$ sizes are given by the same formula:

$$\text{overhead}_\text{CoverCrypt} =  \text{size}_\text{encapsulation} + \text{size}_\text{LEB128} + \text{size}_\text{encrypted additional data}$$

where:

$$\text{size}_\text{encapsulation} = 2 \times \text{size}_\text{public key} + \text{size}_\text{tag} + \text{size}_\text{LEB128}(\text{number of partitions}) + \text{encapsulations length} $$

with:

- $\text{size}_\text{public key}$ is 32 bytes
- $\text{size}_\text{tag}$ is 32 bytes
- $\text{size}_\text{LEB128}(n)$ is equal to 1 byte if `n` is less than `2^7`

$$\text{encapsulations length} = \text{number of pre-quantum partitions} \times (1 + \text{size}_\text{public key}) + \text{number of post-quantum partitions} \times (1 + \text{size}_\text{INDCPA KYBER CIPHERTEXT})$$

- $\text{number of pre-quantum partitions}$ is the number of combination of pre-quantum `partitions` of the *encryption policy*
- $\text{number of post-quantum partitions}$ is the number of combination of post-quantum `partitions` of the *encryption policy*
- $\text{size}_\text{INDCPA KYBER CIPHERTEXT}$ is the constant `1088` given by the `Kyber` scheme[^1]
- optional `additional_data` can be useful to store metadata (but not used in this `Spark` implementation, hence $\text{size}_\text{encrypted additional data} = 0$)

[^1]: notations come from [CoverCrypt readme and implementation](https://github.com/Cosmian/cover_crypt) and [CoverCrypt scheme](https://github.com/Cosmian/cover_crypt#documentation)

## Examples

### Encrypted footer mode only

Given the following `CoverCrypt` policy (without post-quantum resistance):

```scala
val policy = new Policy(
    20,
    Array[PolicyAxis](
        new PolicyAxis(
            "Country",
            Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("France", false),
            new PolicyAxisAttribute("Germany", false),
            new PolicyAxisAttribute("Others", false)
            ),
            false
        ),
        new PolicyAxis(
            "Security",
            Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("None", false),
            new PolicyAxisAttribute("Basic", false),
            new PolicyAxisAttribute("Secret", false)
            ),
            true
        )
    )
)
```

And considering than:

- files encryption policy is `Country::France` and no metadata is used for encryption,
- the dataset is 14MB (100000-lines dataset),
- Spark is run on a 16-cores CPU machine (given 1568 output `.parquet` files),

&rarr; the total asymmetric cryptographic overhead is:

$$
\begin{aligned}
\text{overhead}_\text{total} &= \text{number of encrypted files}* \text{overhead}_\text{file}\\
  &= 1568 \times ( \text{overhead}_{\text{file footer mode}} + \text{number of encrypted columns} \times \text{overhead}_{\text{column}})\\
  &= 1568 \times (\text{overhead}_{\text{file footer mode}} + 0 \times \text{overhead}_{\text{column}})\\
  &= 1568 \times \text{overhead}_{\text{CoverCrypt}}\\
  &= 1568 \times ( 2 \times 32 + 32 + 1 + 3 \times(1 + 32) + 1) \\
  &= 1568\times 197 \\
  &= 308896 \text{ bytes}
\end{aligned}
$$

&rarr; the total asymmetric cryptographic overhead represents ~1,272% of the total size of output files (`.parquet` and `.crc` files included for a total amount of 24MB or 24282137 bytes).

### Files and columns encryption

Given the same `CoverCrypt` [policy](#encrypted-footer-mode-only) of the previous example and considering than:

- files encryption policy is `Country::France` and no metadata is used for encryption,
- *moreover column `Organization Id` is also encrypted with the encryption policy `Security::Basic`*,
- the dataset is 14MB (100000-lines dataset),
- Spark is run on a 16-cores CPU machine (given 1568 output `.parquet` files),

&rarr; the total asymmetric cryptographic overhead is doubled this time:

$$
\begin{aligned}
\text{overhead}_\text{total} &= \text{number of encrypted files} \times \text{overhead}_\text{file}\\
  &= 1568 \times (\text{overhead}_{\text{file footer mode}} + \text{number of encrypted columns} \times \text{overhead}_{\text{column}})\\
  &= 1568 \times (\text{overhead}_{\text{file footer mode}} + 1 \times \text{overhead}_{\text{column}})\\
  &= 1568 \times \text{overhead}_{\text{CoverCrypt}} \times 2\\
  &= 308896 \times 2\\
  &= 617792 \text{ bytes}
\end{aligned}
$$

&rarr; the total asymmetric cryptographic overhead represents ~3,066% of the total size of output files (`.parquet` and `.crc` files included for a total amount of 20MB or 20149649 bytes).

### Post-quantum Resistant Policy

Given the `CoverCrypt` *post-quantum resistant policy* (resistant for the attribute `Security::Secret`):

```scala
val policy = new Policy(
    20,
    Array[PolicyAxis](
        new PolicyAxis(
            "Country",
            Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("France", false),
            new PolicyAxisAttribute("Germany", false),
            new PolicyAxisAttribute("Others", false)
            ),
            false
        ),
        new PolicyAxis(
            "Security",
            Array[PolicyAxisAttribute](
            new PolicyAxisAttribute("None", false),
            new PolicyAxisAttribute("Basic", false),
            new PolicyAxisAttribute("Secret", true)
            ),
            true
        )
    )
)
```

And considering than:

- files encryption policy is `Country::France` and no metadata is used for encryption,
- the dataset is 14MB (100000-lines dataset),
- Spark is run on a 16-cores CPU machine (given 1568 output `.parquet` files),

&rarr; the total asymmetric cryptographic overhead is:

$$
\begin{aligned}
  \text{overhead}_\text{total} &= \text{number of encrypted files} \times \text{overhead}_\text{file}\\
  &= 1568 \times ( \text{overhead}_{\text{file footer mode}} + \text{number of encrypted columns} \times \text{overhead}_{\text{column}})\\
  &= 1568 \times (\text{overhead}_{\text{file footer mode}} + 0 \times \text{overhead}_{\text{column}})\\
  &= 1568 \times \text{overhead}_{\text{CoverCrypt}}\\
  &= 1568 \times(2\times 32 + 32 + 1 + (1 + 1088 + 2 \times(1 + 32)) + 1) \\
  &= 1568\times 1253\\
  &= 1964704 \text{ bytes}
\end{aligned}
$$

&rarr; the total asymmetric cryptographic overhead represents ~9,004% of the total size of output files (`.parquet` and `.crc` files included for a total amount of 21MB or 21818389 bytes).

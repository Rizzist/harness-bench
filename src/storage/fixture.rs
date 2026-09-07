//! The pinned economy contents reused by storage without model warm-up.
use super::*;

pub const SEEDS: [(&str, usize, &str); 5] = [
    (
        "alpha architecture notes and stable constraints\n",
        768,
        "873d44d4df38b44114dd315f87e4a505f01777c6c7040d034f76e2a72e0c99ed",
    ),
    (
        "bravo interface notes and deterministic inputs\n",
        752,
        "8a4b66e7675c62320d18099476e562d01ea3f2ee16a23572fc2f7f87a30deb49",
    ),
    (
        "charlie verification notes and expected outputs\n",
        768,
        "622b67801f70ab1fb642f94d5f0dde7e83ae0ca88780b629dc6df4a7a68f6e04",
    ),
    (
        "delta edge cases and bounded failure behavior\n",
        736,
        "16ee6c9b1e57f9b0a1a5d9f6632c1536f3438a39bec2b209881229d8aaf31c4e",
    ),
    (
        "echo integration notes and terminal conditions\n",
        752,
        "4df3359265e9838c07d12a050ead67f79a7df7e02ff83022933dd35e1a0fd53d",
    ),
];
pub const RESPONSE_SHA256: &str =
    "b97fe6d2349a0c3d4e49df9916f57fd83ff8473142eabf5b086f0263e8156375";

pub fn contents() -> Result<Vec<(String, String)>> {
    let mut contents = Vec::new();
    for (i, (text, len, hash)) in SEEDS.iter().enumerate() {
        let content = text.repeat(16);
        if content.len() != *len || format!("{:x}", Sha256::digest(content.as_bytes())) != *hash {
            return Err(AhrbError::Validation("storage pinned seed mismatch".into()));
        }
        contents.push((char::from(b'a' + i as u8).to_string(), content));
    }
    if crate::economy::ECONOMY_OUTPUT_CONTENT.len() != 29
        || format!(
            "{:x}",
            Sha256::digest(crate::economy::ECONOMY_OUTPUT_CONTENT)
        ) != RESPONSE_SHA256
    {
        return Err(AhrbError::Validation(
            "storage pinned response mismatch".into(),
        ));
    }
    Ok(contents)
}

pub fn prompt(turn: u32) -> Result<String> {
    if !(1..=1000).contains(&turn) {
        return Err(AhrbError::Validation(
            "storage turn outside pinned horizon".into(),
        ));
    }
    let contents = contents()?;
    Ok(format!(
        "{}[[AHRB:scenario={TASK};actor=storage;checkpoint=t{turn:04}]]",
        contents[0].1
    ))
}

pub fn seed(workspace: &std::path::Path) -> Result<()> {
    for (suffix, content) in contents()? {
        let path = workspace.join(format!("context-{suffix}.txt"));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)?;
        std::io::Write::write_all(&mut file, content.as_bytes())?;
        file.sync_all()?;
    }
    Ok(())
}

/// Pins are for a fixed model/request identity. Harness-generated request framing
/// and native tool mappings are recorded separately, never padded to these sizes.
pub fn validate_renderer_pins() -> Result<()> {
    use crate::fake_model::{
        AnthropicMessagesFrontend, ModelResponse, OpenAiChatFrontend, OpenAiResponsesFrontend,
        ProtocolFrontend,
    };
    let pins = [
        (
            1,
            0,
            303,
            "61f85ca4f8f15e5af3f3a00fa0267afa3257f847b8375f354a059a890743412d",
        ),
        (
            1,
            1,
            388,
            "3754688e074905507e5c6119edc45efb1ad3615de3c411fe3d50a600ea4e1ef2",
        ),
        (
            1,
            2,
            256,
            "0fb4709fd4342abef0c6974b26cacfa135e565c3e963407d6a35afe886dfc477",
        ),
        (
            10,
            0,
            518,
            "f4ea653d68fe362788b6673f9c2d087e26a18a775e2718877591568e5dc395c3",
        ),
        (
            10,
            1,
            478,
            "945c9fa84e97d1159bf35870b5bfda858cef445e475cf62d55aeac56af8179cf",
        ),
        (
            10,
            2,
            395,
            "f193cd921f5e6a49d9c96e67b51d887eb8b2ba68cf5e4a3a1d3bf9e826ff3808",
        ),
    ];
    let frontends: [&dyn ProtocolFrontend; 3] = [
        &OpenAiChatFrontend,
        &OpenAiResponsesFrontend,
        &AnthropicMessagesFrontend,
    ];
    for (turn, index, len, hash) in pins {
        let frontend = frontends[index];
        let value = if turn == 1 {
            serde_json::json!({"text":crate::economy::ECONOMY_OUTPUT_CONTENT})
        } else {
            serde_json::json!({"tool_calls":[{"id":"storage-read-0010","name":"read_fixture","arguments":{"path":"context-a.txt","route":"[[AHRB:scenario=ahrb-storage-tiny-turns-v1;actor=storage;checkpoint=t0010-terminal]]"}}]})
        };
        let response = ModelResponse {
            dialect: frontend.dialect().into(),
            model: "ahrb-fake-v1".into(),
            scenario: TASK.into(),
            actor: "storage".into(),
            checkpoint: format!("t{turn:04}"),
            request_hash: "storage-fixture-renderer-v1".into(),
            attempt: 1,
            value,
            fault: None,
            retry: false,
            stream: false,
        };
        let rendered = frontend.render(&response)?;
        if rendered.body.len() != len || format!("{:x}", Sha256::digest(&rendered.body)) != hash {
            return Err(AhrbError::Validation(format!(
                "storage renderer pin mismatch: {} t{turn:04}",
                frontend.dialect()
            )));
        }
    }
    Ok(())
}

// SPDX-License-Identifier: Apache-2.0
pragma solidity 0.8.30;

/// @notice Local event fixture. It does not custody assets or relay messages.
contract MockBridgeSource {
    uint256 public nonce;

    event MessageSent(
        bytes32 messageId,
        uint256 destinationChainId,
        address sender,
        address recipient,
        bytes32 payloadHash,
        address token,
        uint256 amount
    );

    function send(uint256 destinationChainId, address recipient, bytes32 payloadHash)
        external
        returns (bytes32 messageId)
    {
        require(destinationChainId != block.chainid && recipient != address(0), "invalid destination");
        uint256 sequence = ++nonce;
        messageId = keccak256(abi.encode(
            block.chainid, destinationChainId, msg.sender, recipient, payloadHash, sequence
        ));
        emit MessageSent(messageId, destinationChainId, msg.sender, recipient, payloadHash, address(0), 0);
    }
}

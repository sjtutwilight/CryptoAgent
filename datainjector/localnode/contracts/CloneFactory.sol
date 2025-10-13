// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract CloneFactory {
    event CloneCreated(address indexed instance);
    uint256 public counter;
    function clone(address implementation) external returns (address instance) {
        bytes20 targetBytes = bytes20(implementation);

        bytes32 salt = keccak256(abi.encodePacked(msg.sender, counter));
        counter += 1;
        assembly {
            let clone := mload(0x40)

            mstore(
                clone,
                0x3d602d80600a3d3981f3363d3d373d3d3d363d73000000000000000000000000
            )
            mstore(add(clone, 0x14), targetBytes)
            mstore(
                add(clone, 0x28),
                0x5af43d82803e903d91602b57fd5bf30000000000000000000000000000000000
            )
            instance := create2(0, clone, 0x37, salt)
        }

        require(instance != address(0), "CloneFactory: Failed to create clone");
        emit CloneCreated(instance);
    }
}

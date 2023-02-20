package org.opensearch.dataprepper.plugins.source;

public class S3SelectException extends RuntimeException{
    
	private static final long serialVersionUID = -5467105614601384937L;

	S3SelectException(final String message){
        super(message);
    }
}
